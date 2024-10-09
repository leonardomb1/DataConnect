using DataConnect.Etl.Sql;
using DataConnect.Models;
using DataConnect.Shared;
using DataConnect.Types;
using Microsoft.Data.SqlClient;
using System.Data;

namespace DataConnect.Etl.Templates;

public static class MssqlDataTransfer
{
    public static async Task<Result<int, Error>> ExchangeData(string conStr,
                                                              int scheduleId,
                                                              int systemId,
                                                              int maxTableCount,
                                                              int packetSize,
                                                              string authSecret)
    {
        int errCount = 0;

        using SqlServerCall homeServerCall = new(conStr);
        var options = new ParallelOptions { MaxDegreeOfParallelism = maxTableCount };

        // Reservar id de execução no banco de dados
        var getExecutionIdAttempt = await homeServerCall.GetScalarDataFromServer(
            @$"
            INSERT INTO DW_EXECUCAO (ID_DW_AGENDADOR)
            OUTPUT INSERTED.ID_DW_EXECUCAO
            VALUES ({scheduleId});",
            "DWController"
        );
        if(!getExecutionIdAttempt.IsOk) return getExecutionIdAttempt.Error;
        int executionId = getExecutionIdAttempt.Value; 

        // Resgatar metadados da extração de dados
        var getMetadataAttempt = await homeServerCall.GetTableDataFromServer(
                @$"
                SELECT 
                    EXT.*,
                    SI.NM_SISTEMA,
                    SI.DS_CONSTRING
                FROM DWController..DW_EXTLIST AS EXT WITH(NOLOCK)
                INNER JOIN DWController..DW_SISTEMAS AS SI WITH(NOLOCK)
                    ON  SI.ID_DW_SISTEMA = EXT.ID_DW_SISTEMA
                WHERE   EXT.ID_DW_SISTEMA = {systemId} AND
                        EXT.ID_DW_AGENDADOR = {scheduleId};
                "
        );
        if (!getMetadataAttempt.IsOk) return getMetadataAttempt.Error;
        var metadata = ExtractionMetadata.ConvertFromDataTable(
            getMetadataAttempt.Value,
            authSecret
        );

        // Resgatar consultas a serem executadas
        var getQueryDataAttempt = await homeServerCall.GetTableDataFromServer(
                @$"SELECT 
                    * 
                FROM DWController..DW_CONSULTA WITH(NOLOCK)
                WHERE ID_DW_SISTEMA = {systemId}"
        );
        if(!getQueryDataAttempt.IsOk) return getQueryDataAttempt.Error;
        var queryData = QueryMetadata.ConvertFromDataTable(
            getQueryDataAttempt.Value
        );

        homeServerCall.Dispose();

        await Parallel.ForEachAsync(Enumerable.Range(0, metadata.Count), options, async (i, token) => {
            using var localHomeCall = new SqlServerCall(conStr);
            using var localRemoteCall = new SqlServerCall(metadata[0].ConnectionString!);

            await Log.ToServer(
                $"Table {metadata[i].SystemName}.{metadata[i].TableName} extraction has been queued.", 
                executionId, 
                metadata[i].ExtractId, 
                Constants.LogBeginExecute, 
                localHomeCall
            );

            // Contar linhas do banco para verificar se será realizado extração total ou incremental
            var getLineCountAttempt = await localHomeCall.GetScalarDataFromServer(
                $"SELECT COUNT(1) FROM {metadata[i].SystemName}.{metadata[i].TableName} WITH(NOLOCK);"
            );
            if(!getLineCountAttempt.IsOk) {
                errCount++;
                Log.Out($"Error occurred while attempting to get line count: {getLineCountAttempt.Error.ExceptionMessage}");
                return;
            };
            int lineCount = getLineCountAttempt.Value;
            
            // Criar tabela temporária no servidor remoto
            var createRemoteTableAttempt = await localRemoteCall.ExecuteCommand(
                AddParameter(
                    queryData,
                    metadata[i].TableType, 
                    lineCount, 
                    metadata[i].TableName, 
                    metadata[i].LookBackValue, 
                    metadata[i].ColumnName
                )
            );
            if(!createRemoteTableAttempt.IsOk) {
                errCount++;
                Log.Out($"Error while attempting to create temp table in remote server: {createRemoteTableAttempt.Error.ExceptionMessage}");
                return;
            }

            // Remover os dados que estão na tabela remota, e estão no servidor local
            var cleanLocalTableAttempt = await localHomeCall.ExecuteCommand(
                BuildCommandWithDelete(
                    queryData,
                    metadata[i].TableType,
                    lineCount, 
                    metadata[i].TableName, 
                    metadata[i].IndexName!, 
                    metadata[i].LookBackValue, 
                    metadata[i].ColumnName, 
                    metadata[i].SystemName
                )
            );
            if(!cleanLocalTableAttempt.IsOk) {
                errCount++;
                Log.Out($"Error while attempting to clean local table: {cleanLocalTableAttempt.Error.ExceptionMessage}");
                return;
            }

            // Transitar dados do servidor remoto ao local em pacotes
            var reader = await localRemoteCall.ReadPacketFromServer(
                $"SELECT *, GETDATE() FROM ##T_{metadata[i].TableName}_DW_SEL WITH(NOLOCK);",
                packetSize,
                metadata[i].TableName,
                metadata[i].SystemName,
                localHomeCall,
                executionId,
                metadata[i].ExtractId
            );
            if(!reader.IsOk) {
                errCount++;
                Log.Out($"Error while attempting to transit packet data from remote server: {reader.Error.ExceptionMessage}");
                return;
            }
            
            Log.Out("Finished reading data from server, proceeding with temp table removal.");

            // Remover tabela temporaria no servidor remoto
            var drop = await localRemoteCall.ExecuteCommand(
                new SqlCommand($"DROP TABLE IF EXISTS ##T_{metadata[i].TableName}_DW_SEL")
            );
            if(!drop.IsOk) {
                errCount++;
                Log.Out($"Error while attempting to drop temp table in remote server: {drop.Error.ExceptionMessage}");
                return;
            }

            await Log.ToServer(
                $"Table {metadata[i].SystemName}.{metadata[i].TableName} extraction has been completed.", 
                executionId, 
                metadata[i].ExtractId, 
                Constants.LogEndExecute, 
                localHomeCall
            );
        });

        if(errCount == metadata.Count) {
            return new Error() { ExceptionMessage = "The data extraction attempt has reached maximum error count." };
        }

        if(errCount < metadata.Count && errCount > 0) {
            return new Error {
                ExceptionMessage = "The data extraction attempt was partially successful.",
                IsPartialSuccess = true,
                ErrorCount = errCount
            };
        }

        return Constants.MethodSuccess;  
    }

    private static SqlCommand AddParameter(List<QueryMetadata> queryData,
                                           char tableType,
                                           int lineCount,
                                           string tableName,
                                           int? lookBackValue,
                                           string? columnName)
    {
        var command = new SqlCommand() {
            CommandText = queryData
                .Where(any => any.QueryType == tableType)
                .Select(x => x.QueryText).FirstOrDefault()!,
            CommandTimeout = Constants.HourInSeconds
        };

        switch ((lineCount, tableType))
        {
            case (_, Constants.Total):
                command.Parameters.AddWithValue("@TABELA", tableName);
                break;
            case (0, Constants.Incremental):
                command.CommandText = queryData
                    .Where(any => any.QueryType == Constants.Total)
                    .Select(x => x.QueryText).FirstOrDefault()!;

                command.Parameters.AddWithValue("@TABELA", tableName);
                break;
            case (> 0, Constants.Incremental):
                command.Parameters.AddWithValue("@TABELA", tableName);
                command.Parameters.AddWithValue("@VL_CORTE", lookBackValue.ToString() ?? "0");
                command.Parameters.AddWithValue("@COL_DT", columnName ?? "");
                break;
            default:
                break;
        }

        return command;
    }

    private static SqlCommand BuildCommandWithDelete(List<QueryMetadata> queryData,
                                                     char tableType,
                                                     int lineCount,
                                                     string tableName,
                                                     string indexName,
                                                     int? lookBackValue,
                                                     string? columnName,
                                                     string sysName)
    {
        var command = new SqlCommand() {
            CommandText = queryData
                .Where(any => any.QueryType == Constants.Delete)
                .Select(x => x.QueryText).FirstOrDefault()!,
            CommandTimeout = Constants.HourInSeconds
        };

        switch ((lineCount, tableType))
        {
            case (_, Constants.Total):
                command.CommandText = $"TRUNCATE TABLE {sysName}.{tableName};";
                break;
            case (0, Constants.Incremental):
                command.CommandText = $"TRUNCATE TABLE {sysName}.{tableName};";
                break;
            case (> 0, Constants.Incremental):
                command.Parameters.AddWithValue("@NOMEIND", indexName);
                command.Parameters.AddWithValue("@TABELA", tableName);
                command.Parameters.AddWithValue("@VL_CORTE", lookBackValue.ToString());
                command.Parameters.AddWithValue("@COL_DT", columnName);
                break;
            default:
                break;
        }

        return command;
    }
}
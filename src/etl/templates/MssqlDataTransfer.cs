using DataConnect.Etl.Sql;
using DataConnect.Models;
using DataConnect.Shared;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Reflection.Metadata;

namespace DataConnect.Etl.Templates;

public static class MssqlDataTransfer
{
    public static async Task<int> ExchangeData(string conStr,
                                          int scheduleId,
                                          int systemId,
                                          int threadPagination,
                                          int packetSize)
    {
        List<Task> tasks = [];
        int errCount = 0;


        using SqlServerCall homeServerCall = new(conStr);
        var options = new ParallelOptions { MaxDegreeOfParallelism = threadPagination };

        int executionId = homeServerCall.GetScalarDataFromServer(
            @$"
            INSERT INTO DW_EXECUCAO (ID_DW_AGENDADOR)
            OUTPUT INSERTED.ID_DW_EXECUCAO
            VALUES ({scheduleId});",
            stayAlive:false,
            "DWController"
        ).Result.Value;

        var metadata = ExtractionMetadata.ConvertFromDataTable(
            await homeServerCall.GetTableDataFromServer(
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
            )
        );

        var queryData = QueryMetadata.ConvertFromDataTable(
            await homeServerCall.GetTableDataFromServer(
                @$"SELECT 
                    * 
                FROM DWController..DW_CONSULTA WITH(NOLOCK)
                WHERE ID_DW_SISTEMA = {systemId}"
            )
        );

        await Parallel.ForEachAsync(Enumerable.Range(0, metadata.Count), options, async (i, token) => {
            using var localHomeCall = new SqlServerCall(conStr);
            using var localRemoteCall = new SqlServerCall(metadata[0].ConnectionString);

            await Log.ToServer(
                $"Table {metadata[i].SystemName}.{metadata[i].TableName} extraction has been queued.", 
                executionId, 
                metadata[i].ExtractId, 
                Constants.LogBeginExecute, 
                localHomeCall
            );

            int lineCount = localHomeCall.GetScalarDataFromServer(
                $"SELECT COUNT(1) FROM {metadata[i].SystemName}.{metadata[i].TableName} WITH(NOLOCK);", 
                stayAlive:true
            ).Result.Value;
        
            await localRemoteCall.ExecuteCommand(
                AddParameter(
                    queryData,
                    metadata[i].TableType, 
                    lineCount, 
                    metadata[i].TableName, 
                    metadata[i].LookBackValue, 
                    metadata[i].ColumnName
                ),
                stayAlive:true
            );

            await localHomeCall.ExecuteCommand(
                BuildCommandWithDelete(
                    queryData,
                    metadata[i].TableType,
                    lineCount, 
                    metadata[i].TableName, 
                    metadata[i].IndexName!, 
                    metadata[i].LookBackValue, 
                    metadata[i].ColumnName, 
                    metadata[i].SystemName
                ),
                stayAlive:true
            );

            var reader = await localRemoteCall.ReadPacketFromServer(
                $"SELECT *, GETDATE() FROM ##T_{metadata[i].TableName}_DW_SEL WITH(NOLOCK);",
                packetSize,
                metadata[i].TableName,
                metadata[i].SystemName,
                localHomeCall,
                executionId,
                metadata[i].ExtractId,
                stayAlive:true
            );
            
            Log.Out("Finished reading data from server, proceeding with temp table removal.");

            await localRemoteCall.ExecuteCommand(
                new SqlCommand($"DROP TABLE IF EXISTS ##T_{metadata[i].TableName}_DW_SEL")
            );

            if (!reader.IsOk) {
                Log.Out("Error while attempting to send data to server.");
                errCount++;            
            } else {
                await Log.ToServer(
                    $"Table {metadata[i].SystemName}.{metadata[i].TableName} extraction has been completed.", 
                    executionId, 
                    metadata[i].ExtractId, 
                    Constants.LogEndExecute, 
                    localHomeCall
                );
            }
        });

        if (errCount > 0) return Constants.MethodFail;
        
        return Constants.MethodSuccess;
    }

    private static SqlCommand AddParameter(List<QueryMetadata> queryData, char tableType, int lineCount, string tableName, int? lookBackValue, string? columnName)
    {
        var command = new SqlCommand() {
            CommandText = queryData
                .Where(any => any.QueryType == tableType)
                .Select(x => x.QueryText).FirstOrDefault()!
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

    private static SqlCommand BuildCommandWithDelete(List<QueryMetadata> queryData, char tableType, int lineCount, string tableName, string indexName, int? lookBackValue, string? columnName, string sysName)
    {
        var command = new SqlCommand() {
            CommandText = queryData
                .Where(any => any.QueryType == Constants.Delete)
                .Select(x => x.QueryText).FirstOrDefault()!
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
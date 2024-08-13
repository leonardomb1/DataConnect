# DataConnect

Este projeto é uma aplicação desenvolvida em .NET 8 para execução em um contêiner Docker, destinada à extração de dados de APIs e bancos de dados SQL.

## Documentação

O código foi documentado utilizando a ferramenta DocFX, permitindo que a documentação seja visualizada através de uma interface web. Para visualizar a documentação, siga os passos abaixo:

1. Instale o DocFX:
    ```bash
    dotnet tool install -g docfx
    ```
2. No diretório do projeto, execute:
    ```bash
    cd documentation/ && docfx -s
    ```

## Compilação e Execução

# Compilação
A aplicação pode ser compilada utilizando:

1. Linux Alpine
    ```bash
    dotnet publish -c Release -r linux-musl-x64 --self-contained true /p:PublishSingleFile=true /p:DebugType=None -o out
    ```

2. Linux Ubuntu
    ```bash
    dotnet publish -c Release -r linux-x64 --self-contained true /p:PublishSingleFile=true /p:DebugType=None -o out
    ```

2. Windows x64
    ```bash
    dotnet publish -c Release -r win-x64 --self-contained true /p:PublishSingleFile=true /p:DebugType=None -o out
    ```

# Uso (CLI)
O programa pode ser compilado e executado através da linha de comando (CLI) com as seguintes opções:

1.  Execução em CLI:
    ```bash
    DataConnect [options]
    ```

    `-h`, `--help`  Exibe mensagem de ajuda.

    `-v`, `--version`   Exibe informação da versão da compilação atual.

    `-e`, `--environment` `<port>` `<connection>` `<database>` `<threads>` Define as váriaveis de ambiente necessárias para execução.

# Uso (Docker)
O programa pode ser iniciado com Docker compose ou Portainer:

1.  Execução Docker:
    ```bash
    docker compose create && docker compose start
    ```
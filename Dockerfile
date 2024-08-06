FROM mcr.microsoft.com/dotnet/sdk:8.0-alpine AS build

WORKDIR /app

COPY /src/* ./
RUN dotnet publish -c Release -r linux-musl-x64 --self-contained true /p:PublishSingleFile=true /p:DebugType=None -o out

FROM alpine:latest
RUN apk add --no-cache icu-libs libc6-compat tzdata
RUN cp /usr/share/zoneinfo/America/Sao_Paulo /etc/localtime && echo "America/Sao_Paulo" > /etc/timezone
WORKDIR /app
COPY --from=build /app/out/DataConnect .
RUN chmod +x /app/DataConnect

EXPOSE 10000

ENTRYPOINT ["/app/DataConnect"]
CMD ["tail", "-f", "/dev/null"]

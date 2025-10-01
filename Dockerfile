# Build stage
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

# Copy project file and restore dependencies
COPY src/DataConnect.csproj ./src/
RUN dotnet restore "src/DataConnect.csproj"

# Copy source code and build
COPY src/ ./src/
WORKDIR /src/src
RUN dotnet build "DataConnect.csproj" -c Release -o /app/build

# Publish stage
FROM build AS publish
RUN dotnet publish "DataConnect.csproj" -c Release -o /app/publish --no-restore

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app

# Install curl for health checks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Set timezone
ENV TZ=America/Sao_Paulo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Copy published application
COPY --from=publish /app/publish .

# Create non-root user for security
RUN adduser --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

# Expose port
EXPOSE 40000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:40000/health || exit 1

# Entry point
ENTRYPOINT ["dotnet", "DataConnect.dll"]


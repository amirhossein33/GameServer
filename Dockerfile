FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
# restore first for better layer caching
COPY src/GameServer/GameServer.csproj src/GameServer/
RUN dotnet restore src/GameServer/GameServer.csproj
COPY src/GameServer/ src/GameServer/
RUN dotnet publish src/GameServer/GameServer.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:9.0
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "GameServer.dll"]

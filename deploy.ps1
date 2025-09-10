# PowerShell version: 7.1

param(
        [string]$AppName = "bhsim-app",
        [string]$ResourceGroup = "bhsim-rg",
        [string]$Location = "",
        [string]$RegistryName = "",
        [string]$ImageName = "bhsim",
    [string]$ImageTag = "latest",
        [string]$EventHubConnectionString = "",
        [string]$EventHubName = "",
        [string]$SqlConnectionString = "",
    [switch]$ManagedIdentity,
    [string]$Command = "",
    [string]$SqlServer = "",              # e.g. flightopsdb (without .database.windows.net)
    [string]$SqlDatabase = "",            # e.g. flight_ops_db
    [string]$SqlFlightsTable = "dbo.Flights", # Target table for INSERT permissions
    [switch]$GrantSqlPermissions,          # When set, attempt to create MI user & grant INSERT
    [string]$Dockerfile = "Dockerfile",    # Path to Dockerfile
    [string]$BuildContext = ".",          # Build context for docker
    [switch]$SkipBuildIfExists,            # Skip build if tag already present in ACR
    [switch]$AllowEmptyEventHub,        # Allow running without Event Hub connection string (forces dry-run in app)
        [switch]$NonInteractive,
        [switch]$Help
)

function Show-Usage {
        @"
Deploy Baggage Handling Simulator to Azure Container Instances

Parameters:
    -AppName <name>                Container instance name (default: bhsim-app)
    -ResourceGroup <name>          Resource group (default: bhsim-rg)
    -Location <region>             Azure region (e.g. westeurope)
    -RegistryName <acrName>        Existing ACR name (no FQDN)
    -ImageName <image>             Image repository (default: bhsim)
    -ImageTag <tag>                Image tag (default: latest)
    -EventHubConnectionString <cs> Event Hubs connection string (required unless -AllowEmptyEventHub)
    -EventHubName <hub>            Event Hub name (if not in connection string)
    -SqlConnectionString <cs>      Optional SQL Server ODBC connection string
    -ManagedIdentity               Enable system-assigned managed identity on container instance
    -Command <cmd>                 Override container command (e.g. 'bhsim --clock-speed 1 ...')
    -SqlServer <name>              Azure SQL logical server name (without .database.windows.net) for permission grant
    -SqlDatabase <name>            Azure SQL database name
    -SqlFlightsTable <schema.tbl>  Table to grant INSERT (default: dbo.Flights)
    -GrantSqlPermissions           After deploy, create user for MI & grant INSERT (and SELECT) on table
    (If -SqlConnectionString not supplied but -SqlServer & -SqlDatabase provided, a connection string will be constructed automatically using Managed Identity or Interactive auth.)
    -Dockerfile <path>             Dockerfile path (default: ./Dockerfile)
    -BuildContext <dir>            Docker build context (default: current directory)
    -SkipBuildIfExists             Do not rebuild if the image:tag already exists in ACR
    -AllowEmptyEventHub            Permit deployment without Event Hub connection (app auto --dry-run)
    -NonInteractive                Do not prompt; fail for missing required values (except EventHub when allowed)
    -Help                          Show this help

Examples:
    ./deploy.ps1 -Location westeurope -RegistryName myacr -EventHubConnectionString "Endpoint=...;EntityPath=hub" -ResourceGroup bhsim-rg
"@
}

if ($Help) { Show-Usage; exit 0 }

if (-not $NonInteractive) {
    $changeAppName = Read-Host "`r`nDefault app name is '$AppName'.`r`nProvide a new name or press ENTER to keep the current name"
    if (-not [string]::IsNullOrEmpty($changeAppName)) { $AppName = $changeAppName }
}

if ([string]::IsNullOrEmpty($EventHubConnectionString) -and -not $AllowEmptyEventHub) {
    if (-not $NonInteractive) {
        $EventHubConnectionString = Read-Host "`r`nProvide the Azure Event Hubs connection string (may include EntityPath)"
    }
    if ([string]::IsNullOrEmpty($EventHubConnectionString)) { Write-Host "Event Hubs connection string required (omit or use -AllowEmptyEventHub for dry-run)."; exit 1 }
}

# Try to extract Event Hub name if not provided
if (-not [string]::IsNullOrEmpty($EventHubConnectionString)) {
    if ([string]::IsNullOrEmpty($EventHubName)) {
        if ($EventHubConnectionString -match "EntityPath=([^;]+)") { $EventHubName = $Matches[1] }
    }
    if ([string]::IsNullOrEmpty($EventHubName) -and -not $NonInteractive) { $EventHubName = Read-Host "Enter Event Hub name (if not in connection string)" }
}

if ([string]::IsNullOrEmpty($SqlConnectionString) -and -not $NonInteractive) {
    $SqlConnectionString = Read-Host "(Optional) Provide SQL Server ODBC connection string or press ENTER to skip"
}

# Auto-construct SQL connection string if not provided but server & database specified
if (-not $SqlConnectionString -and $SqlServer -and $SqlDatabase) {
    # Allow user to pass either bare server name or FQDN *.database.windows.net
    if ($SqlServer -match "\.database\.windows\.net$") { $SqlServer = $SqlServer -replace "\.database\.windows\.net$","" }
    $authMode = if ($ManagedIdentity) { 'ActiveDirectoryManagedIdentity' } else { 'ActiveDirectoryInteractive' }
    $SqlConnectionString = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:$SqlServer.database.windows.net,1433;Database=$SqlDatabase;Encrypt=yes;TrustServerCertificate=no;Authentication=$authMode"
}

# Login to Azure if not already logged in
if (-not (az account show)) {
    az login
}

# Prompt for the Location from a list of available locations
if ([string]::IsNullOrEmpty($Location)) {
    if ($NonInteractive) { Write-Host "-Location is required in NonInteractive mode"; exit 1 }
    $locations = az account list-locations --query "[?metadata.regionType == 'Physical'].{Name:name, DisplayName:displayName, Geo: metadata.geographyGroup}" --output json | ConvertFrom-Json | Sort-Object Geo, DisplayName
    $locationChoice = $null
    do {
        Write-Host "`r`nAvailable Locations:"; $priorGeo = ""
        for ($i=0; $i -lt $locations.Count; $i++) {
            $loc = $locations[$i]
            if ($priorGeo -ne $loc.Geo) { Write-Host "`n$($loc.Geo)`n------------------"; $priorGeo = $loc.Geo }
            Write-Host -NoNewLine ("{0,-30}" -f ("{0}. {1}" -f ($i+1), $loc.DisplayName))
            if ($i % 3 -eq 2) { Write-Host }
        }
        $locationNumber = Read-Host "`r`nEnter the number of the desired location"
        if ([int]::TryParse($locationNumber, [ref]$locationChoice) -and $locationChoice -ge 1 -and $locationChoice -le $locations.Count+1) {
            $locationChoice -= 1
        } else {
            Write-Host "Invalid input. Please enter a valid number."
        }
    } while ($null -eq $locationChoice)
    $Location = $locations[$locationChoice].Name
}


# Prompt for the ResourceGroup if empty and validate input
if (-not $NonInteractive) {
    do {
        $changeResourceGroup = Read-Host "`r`nResource group is '$ResourceGroup'.`r`nProvide a new name or press Enter to keep the current name"
        if ([string]::IsNullOrEmpty($changeResourceGroup)) { break }
    } while (-not ($changeResourceGroup -match "^[a-zA-Z0-9_-]+$"))
    if (-not [string]::IsNullOrEmpty($changeResourceGroup)) { $ResourceGroup = $changeResourceGroup }
}


#Prompt for the RegistryName if empty and validate input
if ([string]::IsNullOrEmpty($RegistryName)) {
    if ($NonInteractive) { Write-Host "-RegistryName is required in NonInteractive mode"; exit 1 }
    do { $RegistryName = Read-Host "`r`nEnter the Azure Container Registry name" } while (-not ($RegistryName -match "^[a-zA-Z0-9_-]+$"))
}

# Check if the resource group already exists
$existingResourceGroup = az group show --name $ResourceGroup --query name --output tsv
# Create the resource group if it doesn't exist
if (-not $existingResourceGroup) {
    az group create --name $ResourceGroup --location $Location
}

$AcrName = "$RegistryName.azurecr.io"
# Create a container registry if it doesn't exist
$existingRegistry = az acr show --name $RegistryName --query name --output tsv 2>$null
if (-not $existingRegistry) { Write-Host "Container Registry '$RegistryName' not found."; exit 1 }

# Determine if image:tag exists already (before login to avoid unnecessary auth if skipping build)
$imageExists = $false
try {
    $tagCount = az acr repository show-tags --name $RegistryName --repository $ImageName --query "[?@=='$ImageTag'] | length(@)" -o tsv 2>$null
    if ($tagCount -and [int]$tagCount -gt 0) { $imageExists = $true }
} catch { }

# Build & push image if needed
if (-not $imageExists -or -not $SkipBuildIfExists) {
    $actionMsg = if ($imageExists -and -not $SkipBuildIfExists) { "Rebuilding existing image ${ImageName}:$ImageTag" } else { "Building image ${ImageName}:$ImageTag" }
    Write-Host $actionMsg
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        Write-Host "Docker CLI not found. Install Docker or use 'az acr build' approach."; exit 1
    }
    # Login prior to push (ignore if already logged in)
    az acr login --name $RegistryName | Out-Null
    $fullImage = "$AcrName/${ImageName}:$ImageTag"
    $buildArgs = @('build', '-f', $Dockerfile, '-t', $fullImage, $BuildContext)
    Write-Host "docker $($buildArgs -join ' ')"
    docker @buildArgs
    if ($LASTEXITCODE -ne 0) { Write-Host "Docker build failed."; exit 1 }
    docker push $fullImage
    if ($LASTEXITCODE -ne 0) { Write-Host "Docker push failed."; exit 1 }
} else {
    Write-Host "Image ${ImageName}:$ImageTag already exists in ACR; skipping build (SkipBuildIfExists specified)."
}

# Log in & retrieve credentials (still needed for container create unless using MI)
az acr login --name $RegistryName | Out-Null


$registryPassword = az acr credential show --name $RegistryName --query passwords[0].value --output tsv
$registryUsername = az acr credential show --name $RegistryName --query username --output tsv

$existingContainer = az container show --resource-group $ResourceGroup --name $AppName --query name --output tsv 2>$null
$secureVars = @("EVENTHUB_CONNECTION_STRING=$EventHubConnectionString")
if ($SqlConnectionString) { $secureVars += "SQLSERVER_CONNECTION_STRING=$SqlConnectionString" }
$envArgs = @()
if ($secureVars.Count -gt 0) { $envArgs += @('--secure-environment-variables') + $secureVars }
if ($EventHubName) { $envArgs += @('--environment-variables', "EVENTHUB_NAME=$EventHubName") }
if ($ManagedIdentity) { $envArgs += @('--assign-identity') }

$commandArgs = @()
if ($Command) { $commandArgs = @('--command-line', $Command) }

if (-not $existingContainer) {
    az container create --resource-group $ResourceGroup `
        --name $AppName `
        --image "$AcrName/${ImageName}:$ImageTag" `
        --os-type Linux `
        --cpu 1 `
        --memory 2 `
        --restart-policy Always `
        @envArgs `
        @commandArgs `
        --registry-password $registryPassword `
        --registry-username $registryUsername | Out-Null
    Write-Host "Container '$AppName' created."    
} else {
    az container restart --resource-group $ResourceGroup --name $AppName | Out-Null
    Write-Host "Container '$AppName' restarted."    
}

Write-Host "Deployment complete."

if ($GrantSqlPermissions -and $ManagedIdentity) {
    # Attempt automatic grant ONLY if sqlcmd works with current AAD context; otherwise show manual script.
    if (-not $SqlServer -or -not $SqlDatabase) {
        Write-Warning "GrantSqlPermissions requested but -SqlServer or -SqlDatabase missing. Skipping grant.";
    }
    else {
        # Normalize server name (allow FQDN input)
        $normalizedServer = $SqlServer
        if ($normalizedServer -match "\.database\.windows\.net$") { $normalizedServer = $normalizedServer -replace "\.database\.windows\.net$","" }
        $fullServer = "tcp:$normalizedServer.database.windows.net,1433"
        $tableParts = $SqlFlightsTable.Split('.')
        if ($tableParts.Count -ne 2) { Write-Warning "SqlFlightsTable '$SqlFlightsTable' not schema-qualified (schema.table). Skipping automatic grant."; $tableParts = @('dbo','Flights') }
        $schema = $tableParts[0]; $table = $tableParts[1]

        $sql = @"
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$AppName')
BEGIN
    CREATE USER [$AppName] FROM EXTERNAL PROVIDER;
END;
IF NOT EXISTS (
    SELECT 1 FROM sys.database_permissions p
    JOIN sys.objects o ON p.major_id = o.object_id
    JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id
    WHERE dp.name = N'$AppName' AND o.schema_id = SCHEMA_ID(N'$schema') AND o.name = N'$table' AND p.permission_name = 'INSERT'
)
BEGIN
    GRANT SELECT, INSERT ON OBJECT::[$schema].[$table] TO [$AppName];
END;
"@
        Write-Host "Attempting automatic SQL permission grant using Azure AD access token (user=$AppName, table=$schema.$table)..."
        $token = $null
        try { $token = az account get-access-token --resource https://database.windows.net/ --query accessToken -o tsv 2>$null } catch {}
        if (-not $token) {
            Write-Warning "Could not obtain Azure AD access token. Falling back to manual instructions.";
        } else {
            $connString = "Server=$fullServer;Database=$SqlDatabase;Encrypt=True;TrustServerCertificate=False;";
            $granted = $false
            for ($i=1; $i -le 3 -and -not $granted; $i++) {
                try {
                    $conn = [System.Data.SqlClient.SqlConnection]::new($connString)
                    $conn.AccessToken = $token
                    $conn.Open()
                    $cmd = $conn.CreateCommand(); $cmd.CommandText = $sql; $cmd.CommandTimeout = 30; [void]$cmd.ExecuteNonQuery();
                    $conn.Close(); $granted = $true; Write-Host "SQL permissions ensured.";
                } catch {
                    Write-Warning "Grant attempt $i failed: $($_.Exception.Message)";
                    Start-Sleep -Seconds 5
                }
            }
            if (-not $granted) { Write-Warning "Automatic grant failed after retries." }
        }
        if (-not $granted) {
            $grantScript = @"
-- Connect with Azure AD admin to database [$SqlDatabase]
CREATE USER [$AppName] FROM EXTERNAL PROVIDER;
GRANT SELECT, INSERT ON OBJECT::${SqlFlightsTable} TO [$AppName];
"@
            Write-Host $grantScript
        }
    }
}


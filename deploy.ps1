# PowerShell version: 7.1

param(
    [string]$AppName = "bhsim-app",
    [string]$ResourceGroup = "bhsim-rg",
    [string]$Location = "",
    [string]$ImageName = "bhsim",
    [string]$ImageTag = "latest",
    [string]$EventHubConnectionString = "",
    [string]$EventHubName = "",
    [string]$SqlConnectionString = "",
    [switch]$ManagedIdentity,
    [string]$Command = "",
    [string]$SqlServer = "",
    [string]$SqlDatabase = "",
    [string]$SqlFlightsTable = "dbo.Flights",
    [switch]$GrantSqlPermissions,
    [string]$Dockerfile = "Dockerfile",
    [string]$BuildContext = ".",
    [switch]$SkipBuildIfExists,
    [switch]$AllowEmptyEventHub,
    [switch]$Recreate,
    [int]$StartDelaySeconds = 0,
    [switch]$NonInteractive,
    [switch]$Debug,
    [string]$RegistryName = "",
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
    -ManagedIdentity               (Deprecated – system-assigned managed identity is now ALWAYS enabled)
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
    -Recreate                      If container exists, delete and create anew (forces fresh image pull)
    -StartDelaySeconds <n>         Optional startup delay inserted before simulator begins (helps capture logs)
    -NonInteractive                Do not prompt; fail for missing required values (except EventHub when allowed)
    -Debug                         Start container with debug script (diagnostics, infinite sleep) instead of simulator
    -Help                          Show this help

Examples:
    ./deploy.ps1 -Location westeurope -RegistryName myacr -EventHubConnectionString "Endpoint=...;EntityPath=hub" -ResourceGroup bhsim-rg
"@
}

if ($Help) { Show-Usage; exit 0 }

# Enforce system-assigned managed identity regardless of switch usage (requirement: MUST be enabled)
$ManagedIdentity = $true

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
    # Correct ODBC Driver 18 keyword is ActiveDirectoryMsi (not *ManagedIdentity*)
    $authMode = if ($ManagedIdentity) { 'ActiveDirectoryMsi' } else { 'ActiveDirectoryInteractive' }
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

if ($existingContainer -and $Recreate) {
    Write-Host "Deleting existing container '$AppName' (Recreate specified)..."
    az container delete --resource-group $ResourceGroup --name $AppName --yes | Out-Null
    # Wait until deletion completes
    for ($i=0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 2
        $stillExists = az container show --resource-group $ResourceGroup --name $AppName --query name --output tsv 2>$null
        if (-not $stillExists) { break }
    }
    $existingContainer = $null
}
# NOTE: Using plain environment variables (not secure) so values are visible for troubleshooting.
# For production revert to --secure-environment-variables to avoid exposing secrets.
$envArgs = @()        # will be populated after optional file share logic to avoid duplicate flags
$plainEnv = @()
if ($EventHubConnectionString) { $plainEnv += "EVENTHUB_CONNECTION_STRING=$EventHubConnectionString" }
if ($SqlConnectionString) { $plainEnv += "SQLSERVER_CONNECTION_STRING=$SqlConnectionString" }
if ($EventHubName) { $plainEnv += "EVENTHUB_NAME=$EventHubName" }
if ($SqlFlightsTable) { $plainEnv += "SQLSERVER_FLIGHTS_TABLE=$SqlFlightsTable" }
if ($StartDelaySeconds -gt 0) { $plainEnv += "BHSIM_START_DELAY_SECONDS=$StartDelaySeconds" }

# ------------------------------------------------------------
# Azure File Share (for persistent logs) setup (no new params)
# ------------------------------------------------------------
# Derive a globally unique-ish storage account name from AppName (lowercase, 3-24 chars, alphanumeric)
$storageBase = ($AppName -replace '[^a-zA-Z0-9]', '').ToLower()
if (-not $storageBase) { $storageBase = 'bhsim' }
if ($storageBase.Length -lt 3) { $storageBase = ($storageBase + 'log') }
if ($storageBase.Length -gt 20) { $storageBase = $storageBase.Substring(0,20) }
$StorageAccountName = $storageBase + 'sa'
if ($StorageAccountName.Length -gt 24) { $StorageAccountName = $StorageAccountName.Substring(0,24) }

$shareName = 'bhsimlogs'
$mountPath = '/mnt/share'
$logFile = "$mountPath/bhsim.log"

Write-Host "Ensuring storage account '$StorageAccountName' and file share '$shareName'..."
$created = $false
$attempts = 0
do {
    $attempts++
    $existingStorage = az storage account show --name $StorageAccountName --resource-group $ResourceGroup --query name --output tsv 2>$null
    if (-not $existingStorage) {
        $createOut = az storage account create --name $StorageAccountName --resource-group $ResourceGroup --location $Location --sku Standard_LRS --kind StorageV2 2>&1
        if ($LASTEXITCODE -ne 0) {
            if ($createOut -match 'AlreadyExists') {
                # Append random 4 chars and retry (truncate to stay <=24)
                $suffix = -join ((48..57)+(97..122) | Get-Random -Count 4 | ForEach-Object {[char]$_})
                $base = $storageBase
                if ($base.Length -gt 19) { $base = $base.Substring(0,19) }
                $StorageAccountName = ($base + $suffix)
                Write-Host "Name collision; retrying with '$StorageAccountName'" -ForegroundColor Yellow
                continue
            } else {
                Write-Warning "Failed creating storage account: $createOut"; break
            }
        } else { $created = $true }
    } else { $created = $true }
} while (-not $created -and $attempts -lt 5)

if (-not $created) { Write-Warning "Proceeding without file share mount (storage account creation failed)." }
else {
    $storageKey = az storage account keys list --account-name $StorageAccountName --resource-group $ResourceGroup --query [0].value -o tsv 2>$null
    if (-not $storageKey) { Write-Warning "Could not retrieve storage key; skipping file share mount." }
    else {
        $shareExists = az storage share list --account-name $StorageAccountName --account-key $storageKey --query "[?name=='$shareName'].name | length(@)" -o tsv 2>$null
        if (-not $shareExists -or [int]$shareExists -eq 0) {
            az storage share create --name $shareName --account-name $StorageAccountName --account-key $storageKey | Out-Null
        }
    # Add env var for log file path (will consolidate later into single flag)
    $plainEnv += "BHSIM_LOG_FILE=$logFile"
        # Prepare volume args
        $volumeArgs = @('--azure-file-volume-account-name', $StorageAccountName, '--azure-file-volume-account-key', $storageKey, '--azure-file-volume-share-name', $shareName, '--azure-file-volume-mount-path', $mountPath)
    }
}

# Finalize environment variable arguments (single --environment-variables flag to avoid overwriting)
if ($plainEnv.Count -gt 0) { $envArgs += @('--environment-variables') + $plainEnv }
if ($ManagedIdentity) { $envArgs += @('--assign-identity') }

# -----------------------------
# Debug Mode Preparation
# -----------------------------
if ($Debug) {
    Write-Host "Debug mode enabled: generating diagnostic script..." -ForegroundColor Yellow
    # Ensure we have a mounted share (otherwise create proceeds but script won't persist logs)
    if (-not $volumeArgs) { Write-Warning "Debug mode requested but file share volume not prepared; continuing without persistent logs." }
    $debugLocal = Join-Path -Path ([System.IO.Path]::GetTempPath()) -ChildPath "bhsim-debug-entry.sh"
    $debugScript = @'
#!/bin/sh
set -u
echo "[debug] start $(date -u)" | tee -a /mnt/share/debug.log
echo "[debug] uid=$(id -u) gid=$(id -g) whoami=$(whoami 2>/dev/null || echo n/a)" | tee -a /mnt/share/debug.log
echo "[debug] environment (subset):" | tee -a /mnt/share/debug.log
env | grep -E 'EVENTHUB|SQLSERVER|BHSIM' | tee -a /mnt/share/debug.log || true
echo "[debug] dns lookup sql:" | tee -a /mnt/share/debug.log
getent hosts ${SQLSERVER_CONNECTION_STRING##*Server=tcp:} 2>/dev/null | tee -a /mnt/share/debug.log || nslookup flightopsdb.database.windows.net 2>&1 | tee -a /mnt/share/debug.log || true
echo "[debug] attempting IMDS token (Managed Identity)" | tee -a /mnt/share/debug.log
curl -s -H Metadata:true "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://database.windows.net/" | sed 's/"access_token":"[^"]*"/"access_token":"<redacted>"/' | tee -a /mnt/share/debug.log || echo "[debug] IMDS call failed" | tee -a /mnt/share/debug.log
echo "[debug] ODBC driver libs:" | tee -a /mnt/share/debug.log
ls -1 /opt/microsoft/msodbcsql*/lib 2>/dev/null | tee -a /mnt/share/debug.log || echo "[debug] msodbcsql libs not found" | tee -a /mnt/share/debug.log
echo "[debug] python connectivity test with enhanced diagnostics" | tee -a /mnt/share/debug.log
python - <<'PYCODE' 2>&1 | tee -a /mnt/share/debug.log
import os, traceback, json, base64, time
try:
    import pyodbc
except Exception as e:
    print('[py] pyodbc import failed:', e); traceback.print_exc(); pyodbc=None
cs=os.environ.get('SQLSERVER_CONNECTION_STRING')
print('[py] connection string (original):', cs)
# Get and decode token first for identity analysis
import urllib.request
token=None
try:
    req=urllib.request.Request('http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://database.windows.net/', headers={'Metadata':'true'})
    with urllib.request.urlopen(req, timeout=5) as resp: 
        data=json.loads(resp.read().decode('utf-8'))
    token=data.get('access_token')
    if token:
        header,payload,sig=token.split('.')
        def pad_b64(s): return s + '=' * (4 - len(s) % 4)
        try:
            header_data=json.loads(base64.urlsafe_b64decode(pad_b64(header)))
            payload_data=json.loads(base64.urlsafe_b64decode(pad_b64(payload)))
            print('[py] token header:', header_data)
            print('[py] token claims:', {k:payload_data.get(k) for k in ['oid','appid','sub','tid','aud','unique_name','upn']})
        except Exception as de:
            print('[py] token decode error:', de)
except Exception as e:
    print('[py] IMDS token fetch failed:', e); traceback.print_exc()
# Try direct MSI authentication with extended retry
if pyodbc and cs:
    for attempt in range(1,6):
        try:
            conn=pyodbc.connect(cs, timeout=10)
            cur=conn.cursor(); cur.execute('SELECT TOP 1 SUSER_SNAME(), ORIGINAL_LOGIN(), DB_NAME()')
            print('[py] direct MSI login SUCCESS attempt', attempt, cur.fetchone())
            cur.execute("SELECT name,type_desc,authentication_type_desc FROM sys.database_principals WHERE type='E'")
            print('[py] all external users:', cur.fetchall())
            conn.close(); break
        except Exception as e:
            print(f'[py] direct MSI attempt {attempt} failed:', e); time.sleep(3)
# Try manual token attrs with extended retry
if token and pyodbc and cs:
    import struct
    token_bytes=token.encode('utf-8')
    exptoken=b''.join(bytes([b])+b'\x00' for b in token_bytes)
    token_struct=struct.pack('=i', len(exptoken)) + exptoken
    SQL_COPT_SS_ACCESS_TOKEN=1256
    parts=[p for p in cs.split(';') if p and not p.strip().lower().startswith('authentication=')]
    clean=';'.join(parts)
    print('[py] cleaned connection string:', clean)
    for attempt in range(1,6):
        try:
            conn2=pyodbc.connect(clean, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}, timeout=10)
            cur2=conn2.cursor(); cur2.execute('SELECT SUSER_SNAME(), ORIGINAL_LOGIN(), DB_NAME()')
            print('[py] token attrs login SUCCESS attempt', attempt, cur2.fetchone())
            cur2.execute("SELECT name,type_desc,authentication_type_desc FROM sys.database_principals WHERE type='E'")
            print('[py] external users from token conn:', cur2.fetchall())
            conn2.close(); break
        except Exception as e:
            print(f'[py] token attrs attempt {attempt} failed:', e); time.sleep(3)
PYCODE
echo "[debug] entering sleep (tail)" | tee -a /mnt/share/debug.log
tail -F /dev/null
'@
    # Write with LF line endings
    ($debugScript -replace "`r", "") | Set-Content -Encoding UTF8 -NoNewline $debugLocal
    if ($volumeArgs) {
        try {
            $storageKey = $storageKey # reuse if available
            if (-not $storageKey) {
                $storageKey = az storage account keys list --account-name $StorageAccountName --resource-group $ResourceGroup --query [0].value -o tsv 2>$null
            }
            if ($storageKey) {
                az storage file upload --account-name $StorageAccountName --account-key $storageKey --share-name $shareName --source $debugLocal --path debug-entry.sh --content-type text/x-shellscript 1>$null 2>$null
            }
        } catch { Write-Warning "Failed uploading debug script: $($_.Exception.Message)" }
    }
    # Override command irrespective of user supplied -Command
    $Command = "/bin/sh -c 'cp /mnt/share/debug-entry.sh /tmp/debug-entry.sh && chmod +x /tmp/debug-entry.sh && /tmp/debug-entry.sh'"
}

$commandArgs = @()
if ($Command) { $commandArgs = @('--command-line', $Command) }

$restartPolicy = if ($Debug) { 'Never' } else { 'Always' }

if (-not $existingContainer) {
    az container create --resource-group $ResourceGroup `
        --name $AppName `
        --image "$AcrName/${ImageName}:$ImageTag" `
        --os-type Linux `
        --cpu 1 `
        --memory 2 `
        --restart-policy $restartPolicy `
        @envArgs `
    @volumeArgs `
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

        # Attempt to discover the managed identity object id & display name (may differ from container name)
        $miPrincipalId = $null; $miDisplayName = $null; $miAppId = $null
        try { $miPrincipalId = az container show --resource-group $ResourceGroup --name $AppName --query identity.principalId -o tsv 2>$null } catch {}
        if ($miPrincipalId) {
            try { 
                $spInfo = az ad sp show --id $miPrincipalId --query "{displayName: displayName, appId: appId}" -o json 2>$null | ConvertFrom-Json
                $miDisplayName = $spInfo.displayName
                $miAppId = $spInfo.appId
            } catch {}
        }
        Write-Host "Managed Identity: principalId=$miPrincipalId, displayName=$miDisplayName, appId=$miAppId" -ForegroundColor Yellow

        # For system-assigned managed identity, use the Object ID as name (token presents using oid claim)
        $userIdentity = if ($miPrincipalId) { $miPrincipalId } else { $AppName }
        Write-Host "Creating SQL user with identity: $userIdentity (principalId=$miPrincipalId, displayName=$miDisplayName, appId=$miAppId)" -ForegroundColor Cyan
        
        # Object ID will be converted to proper binary SID format in SQL using CONVERT function
        
        # Use SID approach from Microsoft documentation - bypasses Directory Readers requirement
        # NOTE: If authentication still fails, SQL Server MI needs Directory Readers role:
        # az ad directory-role member add --role "Directory Readers" --member-id "ae03a44b-c55c-435e-a5f8-b824240ba169"
        $sidHex = "0x" + ([System.Guid]::Parse($userIdentity).ToByteArray() | ForEach-Object { $_.ToString("X2") } | Join-String)
        
        $sql = @"
-- Drop existing users to handle principal ID changes
IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$userIdentity' AND type = 'E')
    DROP USER [$userIdentity];
IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$AppName' AND type = 'E')
    DROP USER [$AppName];
IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$miPrincipalId' AND type = 'E')
    DROP USER [$miPrincipalId];

-- Create user using SID approach (bypasses FROM EXTERNAL PROVIDER validation)  
CREATE USER [$userIdentity] WITH SID = $sidHex, TYPE = E;

-- Grant permissions
GRANT SELECT, INSERT ON OBJECT::[$schema].[$table] TO [$userIdentity];

-- List all external users for diagnostics  
SELECT name, type_desc, authentication_type_desc, sid, CAST(sid AS UNIQUEIDENTIFIER) as sid_as_guid FROM sys.database_principals WHERE type = 'E';
"@
        Write-Host "Attempting automatic SQL permission grant using Azure AD access token (user=$userIdentity, table=$schema.$table)..."
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
                    $cmd = $conn.CreateCommand(); $cmd.CommandText = $sql; $cmd.CommandTimeout = 60;
                    [void]$cmd.ExecuteNonQuery();
                    # Post-check to confirm user now has required permissions
                    $checkCmd = $conn.CreateCommand();
                    $checkCmd.CommandText = "SELECT COUNT(*) FROM sys.database_principals WHERE name = N'$userIdentity'";
                    $exists = $checkCmd.ExecuteScalar();
                    $conn.Close();
                    if ($exists -gt 0) {
                        $granted = $true; Write-Host "SQL user '$userIdentity' present and permissions ensured.";
                    } else {
                        Write-Warning "User '$userIdentity' still not visible after grant attempt $i.";
                    }
                } catch {
                    Write-Warning "Grant attempt $i failed: $($_.Exception.Message)";
                    if ($_.Exception.InnerException) { Write-Warning "Inner: $($_.Exception.InnerException.Message)" }
                    Write-Host "Retrying in 5s..." -ForegroundColor Yellow
                    Start-Sleep -Seconds 5
                }
            }
            if (-not $granted) { Write-Warning "Automatic grant failed after retries." }
        }
        if (-not $granted) {
            $sidHexManual = "0x" + ([System.Guid]::Parse($userIdentity).ToByteArray() | ForEach-Object { $_.ToString("X2") } | Join-String)
            $grantScript = @"
-- Connect with Azure AD admin to database [$SqlDatabase]
-- Using SID approach (bypasses FROM EXTERNAL PROVIDER validation)
CREATE USER [$userIdentity] WITH SID = $sidHexManual, TYPE = E;
GRANT SELECT, INSERT ON OBJECT::${SqlFlightsTable} TO [$userIdentity];
-- Optional role membership for broader access:
-- ALTER ROLE db_datareader ADD MEMBER [$userIdentity];
-- ALTER ROLE db_datawriter ADD MEMBER [$userIdentity];
"@
            Write-Host $grantScript
            Write-Warning "If login still fails (Error 18456), the managed identity may need to be granted 'User' type in Azure AD or the SQL connection may require additional configuration.";
        }
    }
}


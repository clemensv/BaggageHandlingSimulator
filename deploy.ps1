# PowerShell version: 7.1

param(
        [string]$AppName = "bhsim-app",
        [string]$ResourceGroup = "bhsim-rg",
        [string]$Location = "",
        [string]$RegistryName = "",
        [string]$ImageName = "bhsim",
        [string]$EventHubConnectionString = "",
        [string]$EventHubName = "",
        [string]$SqlConnectionString = "",
    [switch]$ManagedIdentity,
    [string]$Command = "",
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
    -EventHubConnectionString <cs> Event Hubs connection string (required)
    -EventHubName <hub>            Event Hub name (if not in connection string)
    -SqlConnectionString <cs>      Optional SQL Server ODBC connection string
    -ManagedIdentity               Enable system-assigned managed identity on container instance
    -Command <cmd>                 Override container command (e.g. 'bhsim --clock-speed 1 ...')
    -NonInteractive                Do not prompt; fail for missing required values
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

if ([string]::IsNullOrEmpty($EventHubConnectionString) -and -not $NonInteractive) {
    $EventHubConnectionString = Read-Host "`r`nProvide the Azure Event Hubs connection string (may include EntityPath)"
}
if ([string]::IsNullOrEmpty($EventHubConnectionString)) { Write-Host "Event Hubs connection string required."; exit 1 }

# Try to extract Event Hub name if not provided
if ([string]::IsNullOrEmpty($EventHubName)) {
    if ($EventHubConnectionString -match "EntityPath=([^;]+)") { $EventHubName = $Matches[1] }
}
if ([string]::IsNullOrEmpty($EventHubName)) { $EventHubName = Read-Host "Enter Event Hub name (if not in connection string)" }

if ([string]::IsNullOrEmpty($SqlConnectionString) -and -not $NonInteractive) {
    $SqlConnectionString = Read-Host "(Optional) Provide SQL Server ODBC connection string or press ENTER to skip"
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

# Log in to the Azure Container Registry
az acr login --name $RegistryName


$registryPassword = az acr credential show --name $RegistryName --query passwords[0].value --output tsv
$registryUsername = az acr credential show --name $RegistryName --query username --output tsv

$existingContainer = az container show --resource-group $ResourceGroup --name $AppName --query name --output tsv 2>$null
$envArgs = @('--secure-environment-variables', "EVENTHUB_CONNECTION_STRING=$EventHubConnectionString")
if ($EventHubName) { $envArgs += @('--environment-variables', "EVENTHUB_NAME=$EventHubName") }
if ($SqlConnectionString) { $envArgs += @('--secure-environment-variables', "SQLSERVER_CONNECTION_STRING=$SqlConnectionString") }
if ($ManagedIdentity) { $envArgs += @('--assign-identity') }

$commandArgs = @()
if ($Command) { $commandArgs = @('--command-line', $Command) }

if (-not $existingContainer) {
    az container create --resource-group $ResourceGroup `
        --name $AppName `
        --image "$AcrName/${ImageName}:latest" `
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


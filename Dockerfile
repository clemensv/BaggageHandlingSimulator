# SPDX-License-Identifier: MIT
# Base image
FROM python:3.10-slim

# OCI metadata
LABEL org.opencontainers.image.source="https://github.com/clemensv/BaggageHandlingSimulator"
LABEL org.opencontainers.image.title="Airport Baggage Handling Simulator (CloudEvents)"
LABEL org.opencontainers.image.description="Simulates airport flight, passenger, and baggage handling operations; publishes CloudEvents to Azure Event Hubs and optionally records flight lifecycle markers in SQL Server."
LABEL org.opencontainers.image.documentation="https://github.com/clemensv/BaggageHandlingSimulator/blob/main/CONTAINER.md"
LABEL org.opencontainers.image.license="MIT"

# Set the working directory in the container
WORKDIR /app

# Copy only the files required to install and run the simulator
COPY pyproject.toml ./
COPY src/ ./src/
# Runtime schema/message definitions (lightweight validation)
COPY message-definitions/ ./message-definitions/

RUN pip install --no-cache-dir .

# Define runtime configuration environment variables (all optional; CLI flags override)
ENV EVENTHUB_CONNECTION_STRING="" \
	EVENTHUB_NAME="" \
	SQLSERVER_CONNECTION_STRING="" \
	SIM_DURATION_MINUTES="0" \
	SIM_CLOCK_SPEED="60.0" \
	SIM_FLIGHT_INTERVAL_MINUTES="5" \
	SIM_MAX_ACTIVE_FLIGHTS="0" \
	SIM_LOSS_RATE="0.002" \
	SIM_INSPECT_RATE="0.01" \
	SIM_REJECT_RATE="0.003" \
	SIM_NOT_COLLECTED_RATE="0.005" \
	SIM_CE_MODE="structured"

# Use entrypoint so additional CLI args can be appended at docker run time. Duration defaults via env var.
ENTRYPOINT ["bhsim"]
CMD []
-- SPDX-License-Identifier: MIT
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Flights' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.Flights (
        FlightId        varchar(64)   NOT NULL PRIMARY KEY,
        Airline         varchar(8)    NOT NULL,
        FlightNumber    varchar(16)   NOT NULL,
        Aircraft        varchar(16)   NOT NULL,
        Origin          char(3)       NOT NULL,
        Destination     char(3)       NOT NULL,
        DepartureUtc    datetime2(7)  NOT NULL,
        ArrivalUtc      datetime2(7)  NOT NULL,
        ActualDepartureUtc datetime2(7)  NULL,
        ActualArrivalUtc   datetime2(7)  NULL,
        CheckinClosedUtc   datetime2(7)  NULL,
        CompletedUtc       datetime2(7)  NULL,
        Capacity        int           NOT NULL,
        CreatedUtc      datetime2(7)  NOT NULL DEFAULT (SYSUTCDATETIME())
    );
END
GO

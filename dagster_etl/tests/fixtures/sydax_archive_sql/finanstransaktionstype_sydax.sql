MERGE [arc].[finanstransaktionstype_sydax] AS tgt
USING [ext].[finanstransaktionstype_sydax_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[Transaktionstypenummer] = src.[Transaktionstypenummer]
WHEN MATCHED THEN UPDATE SET
        tgt.[FinanstransaktionstypeId] = src.[FinanstransaktionstypeId],
        tgt.[Transaktionstypetekst] = src.[Transaktionstypetekst],
        tgt.[RunId] = src.[RunId],
        tgt.[StartDateTime] = src.[StartDateTime],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [Transaktionstypenummer], [FinanstransaktionstypeId], [Transaktionstypetekst], [RunId], [StartDateTime], [EndDateTime], [RdwTs], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[Transaktionstypenummer], src.[FinanstransaktionstypeId], src.[Transaktionstypetekst], src.[RunId], src.[StartDateTime], src.[EndDateTime], src.[RdwTs], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

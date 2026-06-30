MERGE [arc].[sydax_finanstransaktionstype] AS tgt
USING [ext].[sydax_finanstransaktionstype_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[FinanstransaktionstypeId] = src.[FinanstransaktionstypeId] AND tgt.[StartDateTime] = src.[StartDateTime]
WHEN MATCHED THEN UPDATE SET
        tgt.[Transaktionstypenummer] = src.[Transaktionstypenummer],
        tgt.[Transaktionstypetekst] = src.[Transaktionstypetekst],
        tgt.[RunId] = src.[RunId],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [FinanstransaktionstypeId], [StartDateTime], [Transaktionstypenummer], [Transaktionstypetekst], [RunId], [EndDateTime], [RdwTs], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[FinanstransaktionstypeId], src.[StartDateTime], src.[Transaktionstypenummer], src.[Transaktionstypetekst], src.[RunId], src.[EndDateTime], src.[RdwTs], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

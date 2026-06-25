MERGE [arc].[gruppering_autoriseret_sydax] AS tgt
USING [ext].[gruppering_autoriseret_sydax_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[ref_RECID] = src.[ref_RECID]
WHEN MATCHED THEN UPDATE SET
        tgt.[G1Id] = src.[G1Id],
        tgt.[G1] = src.[G1],
        tgt.[G1Tekst] = src.[G1Tekst],
        tgt.[F1] = src.[F1],
        tgt.[F2] = src.[F2],
        tgt.[F3] = src.[F3],
        tgt.[D1] = src.[D1],
        tgt.[GyldigFra] = src.[GyldigFra],
        tgt.[GyldigTil] = src.[GyldigTil],
        tgt.[Autoriseret] = src.[Autoriseret],
        tgt.[RunId] = src.[RunId],
        tgt.[StartDateTime] = src.[StartDateTime],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[ref_LedgerTextId] = src.[ref_LedgerTextId],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [ref_RECID], [G1Id], [G1], [G1Tekst], [F1], [F2], [F3], [D1], [GyldigFra], [GyldigTil], [Autoriseret], [RunId], [StartDateTime], [EndDateTime], [RdwTs], [ref_LedgerTextId], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[ref_RECID], src.[G1Id], src.[G1], src.[G1Tekst], src.[F1], src.[F2], src.[F3], src.[D1], src.[GyldigFra], src.[GyldigTil], src.[Autoriseret], src.[RunId], src.[StartDateTime], src.[EndDateTime], src.[RdwTs], src.[ref_LedgerTextId], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

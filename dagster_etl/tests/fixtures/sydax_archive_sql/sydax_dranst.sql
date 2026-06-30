MERGE [arc].[sydax_dranst] AS tgt
USING [ext].[sydax_dranst_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[ref_RECID] = src.[ref_RECID] AND tgt.[StartDateTime] = src.[StartDateTime]
WHEN MATCHED THEN UPDATE SET
        tgt.[DranstId] = src.[DranstId],
        tgt.[D1] = src.[D1],
        tgt.[D1Tekst] = src.[D1Tekst],
        tgt.[GyldigFra] = src.[GyldigFra],
        tgt.[GyldigTil] = src.[GyldigTil],
        tgt.[Autoriseret] = src.[Autoriseret],
        tgt.[ref_LedgerTextId] = src.[ref_LedgerTextId],
        tgt.[RunId] = src.[RunId],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [ref_RECID], [StartDateTime], [DranstId], [D1], [D1Tekst], [GyldigFra], [GyldigTil], [Autoriseret], [ref_LedgerTextId], [RunId], [EndDateTime], [RdwTs], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[ref_RECID], src.[StartDateTime], src.[DranstId], src.[D1], src.[D1Tekst], src.[GyldigFra], src.[GyldigTil], src.[Autoriseret], src.[ref_LedgerTextId], src.[RunId], src.[EndDateTime], src.[RdwTs], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

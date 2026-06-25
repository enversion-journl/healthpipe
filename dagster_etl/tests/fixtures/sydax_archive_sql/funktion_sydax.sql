MERGE [arc].[funktion_sydax] AS tgt
USING [ext].[funktion_sydax_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[ref_RECID] = src.[ref_RECID]
WHEN MATCHED THEN UPDATE SET
        tgt.[FunktionId] = src.[FunktionId],
        tgt.[F1] = src.[F1],
        tgt.[F2] = src.[F2],
        tgt.[F3] = src.[F3],
        tgt.[FunktionTekst] = src.[FunktionTekst],
        tgt.[GyldigFra] = src.[GyldigFra],
        tgt.[GyldigTil] = src.[GyldigTil],
        tgt.[Autoriseret] = src.[Autoriseret],
        tgt.[ref_LedgerTextId] = src.[ref_LedgerTextId],
        tgt.[RunId] = src.[RunId],
        tgt.[StartDateTime] = src.[StartDateTime],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [ref_RECID], [FunktionId], [F1], [F2], [F3], [FunktionTekst], [GyldigFra], [GyldigTil], [Autoriseret], [ref_LedgerTextId], [RunId], [StartDateTime], [EndDateTime], [RdwTs], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[ref_RECID], src.[FunktionId], src.[F1], src.[F2], src.[F3], src.[FunktionTekst], src.[GyldigFra], src.[GyldigTil], src.[Autoriseret], src.[ref_LedgerTextId], src.[RunId], src.[StartDateTime], src.[EndDateTime], src.[RdwTs], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

MERGE [arc].[kreditor_sydax] AS tgt
USING [ext].[kreditor_sydax_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[Kreditornummer] = src.[Kreditornummer]
WHEN MATCHED THEN UPDATE SET
        tgt.[KreditorId] = src.[KreditorId],
        tgt.[Kreditornavn] = src.[Kreditornavn],
        tgt.[Soegenavn] = src.[Soegenavn],
        tgt.[JuridiskEnhed] = src.[JuridiskEnhed],
        tgt.[ErBlokeret] = src.[ErBlokeret],
        tgt.[Oprettelsestidspunkt] = src.[Oprettelsestidspunkt],
        tgt.[OprettetAf] = src.[OprettetAf],
        tgt.[SidstAendretTidspunkt] = src.[SidstAendretTidspunkt],
        tgt.[SidstAendretAf] = src.[SidstAendretAf],
        tgt.[ref_VendtableId] = src.[ref_VendtableId],
        tgt.[RunId] = src.[RunId],
        tgt.[StartDateTime] = src.[StartDateTime],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[DokumentId] = src.[DokumentId],
        tgt.[DataAreaId] = src.[DataAreaId],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [Kreditornummer], [KreditorId], [Kreditornavn], [Soegenavn], [JuridiskEnhed], [ErBlokeret], [Oprettelsestidspunkt], [OprettetAf], [SidstAendretTidspunkt], [SidstAendretAf], [ref_VendtableId], [RunId], [StartDateTime], [EndDateTime], [RdwTs], [DokumentId], [DataAreaId], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[Kreditornummer], src.[KreditorId], src.[Kreditornavn], src.[Soegenavn], src.[JuridiskEnhed], src.[ErBlokeret], src.[Oprettelsestidspunkt], src.[OprettetAf], src.[SidstAendretTidspunkt], src.[SidstAendretAf], src.[ref_VendtableId], src.[RunId], src.[StartDateTime], src.[EndDateTime], src.[RdwTs], src.[DokumentId], src.[DataAreaId], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

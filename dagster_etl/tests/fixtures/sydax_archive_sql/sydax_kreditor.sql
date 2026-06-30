MERGE [arc].[sydax_kreditor] AS tgt
USING [ext].[sydax_kreditor_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[KreditorId] = src.[KreditorId] AND tgt.[StartDateTime] = src.[StartDateTime]
WHEN MATCHED THEN UPDATE SET
        tgt.[Kreditornummer] = src.[Kreditornummer],
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
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[DokumentId] = src.[DokumentId],
        tgt.[DataAreaId] = src.[DataAreaId],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [KreditorId], [StartDateTime], [Kreditornummer], [Kreditornavn], [Soegenavn], [JuridiskEnhed], [ErBlokeret], [Oprettelsestidspunkt], [OprettetAf], [SidstAendretTidspunkt], [SidstAendretAf], [ref_VendtableId], [RunId], [EndDateTime], [RdwTs], [DokumentId], [DataAreaId], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[KreditorId], src.[StartDateTime], src.[Kreditornummer], src.[Kreditornavn], src.[Soegenavn], src.[JuridiskEnhed], src.[ErBlokeret], src.[Oprettelsestidspunkt], src.[OprettetAf], src.[SidstAendretTidspunkt], src.[SidstAendretAf], src.[ref_VendtableId], src.[RunId], src.[EndDateTime], src.[RdwTs], src.[DokumentId], src.[DataAreaId], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

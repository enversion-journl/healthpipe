MERGE [arc].[art_sydax] AS tgt
USING [ext].[art_sydax_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[RECID] = src.[RECID]
WHEN MATCHED THEN UPDATE SET
        tgt.[ArtId] = src.[ArtId],
        tgt.[Artsnummer] = src.[Artsnummer],
        tgt.[Artstekst] = src.[Artstekst],
        tgt.[AktivFra] = src.[AktivFra],
        tgt.[AktivTil] = src.[AktivTil],
        tgt.[Suspenderet] = src.[Suspenderet],
        tgt.[Ejer_id] = src.[Ejer_id],
        tgt.[TilladIkkeManuelAngivelse] = src.[TilladIkkeManuelAngivelse],
        tgt.[A1] = src.[A1],
        tgt.[A2] = src.[A2],
        tgt.[Momsretning] = src.[Momsretning],
        tgt.[RunId] = src.[RunId],
        tgt.[StartDateTime] = src.[StartDateTime],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[ref_DimensionAttributeValueId] = src.[ref_DimensionAttributeValueId],
        tgt.[ArtDimension] = src.[ArtDimension],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [RECID], [ArtId], [Artsnummer], [Artstekst], [AktivFra], [AktivTil], [Suspenderet], [Ejer_id], [TilladIkkeManuelAngivelse], [A1], [A2], [Momsretning], [RunId], [StartDateTime], [EndDateTime], [RdwTs], [ref_DimensionAttributeValueId], [ArtDimension], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[RECID], src.[ArtId], src.[Artsnummer], src.[Artstekst], src.[AktivFra], src.[AktivTil], src.[Suspenderet], src.[Ejer_id], src.[TilladIkkeManuelAngivelse], src.[A1], src.[A2], src.[Momsretning], src.[RunId], src.[StartDateTime], src.[EndDateTime], src.[RdwTs], src.[ref_DimensionAttributeValueId], src.[ArtDimension], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

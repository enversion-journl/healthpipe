MERGE [arc].[sydax_gruppering] AS tgt
USING [ext].[sydax_gruppering_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[RECID] = src.[RECID] AND tgt.[StartDateTime] = src.[StartDateTime]
WHEN MATCHED THEN UPDATE SET
        tgt.[GrupperingId] = src.[GrupperingId],
        tgt.[Grupperingsnummer] = src.[Grupperingsnummer],
        tgt.[Grupperingstekst] = src.[Grupperingstekst],
        tgt.[GrupperingDimension] = src.[GrupperingDimension],
        tgt.[AktivFra] = src.[AktivFra],
        tgt.[AktivTil] = src.[AktivTil],
        tgt.[Suspenderet] = src.[Suspenderet],
        tgt.[Ejer_id] = src.[Ejer_id],
        tgt.[TilladIkkeManuelAngivelse] = src.[TilladIkkeManuelAngivelse],
        tgt.[Ejerforhold] = src.[Ejerforhold],
        tgt.[RunId] = src.[RunId],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[ref_DimensionAttributeValueId] = src.[ref_DimensionAttributeValueId],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [RECID], [StartDateTime], [GrupperingId], [Grupperingsnummer], [Grupperingstekst], [GrupperingDimension], [AktivFra], [AktivTil], [Suspenderet], [Ejer_id], [TilladIkkeManuelAngivelse], [Ejerforhold], [RunId], [EndDateTime], [RdwTs], [ref_DimensionAttributeValueId], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[RECID], src.[StartDateTime], src.[GrupperingId], src.[Grupperingsnummer], src.[Grupperingstekst], src.[GrupperingDimension], src.[AktivFra], src.[AktivTil], src.[Suspenderet], src.[Ejer_id], src.[TilladIkkeManuelAngivelse], src.[Ejerforhold], src.[RunId], src.[EndDateTime], src.[RdwTs], src.[ref_DimensionAttributeValueId], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

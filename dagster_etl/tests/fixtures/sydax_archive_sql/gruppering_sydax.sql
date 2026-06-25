MERGE [arc].[gruppering_sydax] AS tgt
USING [ext].[gruppering_sydax_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[RECID] = src.[RECID]
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
        tgt.[StartDateTime] = src.[StartDateTime],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[ref_DimensionAttributeValueId] = src.[ref_DimensionAttributeValueId],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [RECID], [GrupperingId], [Grupperingsnummer], [Grupperingstekst], [GrupperingDimension], [AktivFra], [AktivTil], [Suspenderet], [Ejer_id], [TilladIkkeManuelAngivelse], [Ejerforhold], [RunId], [StartDateTime], [EndDateTime], [RdwTs], [ref_DimensionAttributeValueId], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[RECID], src.[GrupperingId], src.[Grupperingsnummer], src.[Grupperingstekst], src.[GrupperingDimension], src.[AktivFra], src.[AktivTil], src.[Suspenderet], src.[Ejer_id], src.[TilladIkkeManuelAngivelse], src.[Ejerforhold], src.[RunId], src.[StartDateTime], src.[EndDateTime], src.[RdwTs], src.[ref_DimensionAttributeValueId], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

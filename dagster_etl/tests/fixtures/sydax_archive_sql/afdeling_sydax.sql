MERGE [arc].[afdeling_sydax] AS tgt
USING [ext].[afdeling_sydax_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[Afdelingsnummer] = src.[Afdelingsnummer]
WHEN MATCHED THEN UPDATE SET
        tgt.[AfdelingId] = src.[AfdelingId],
        tgt.[Afdelingstekst] = src.[Afdelingstekst],
        tgt.[AktivFra] = src.[AktivFra],
        tgt.[AktivTil] = src.[AktivTil],
        tgt.[Suspenderet] = src.[Suspenderet],
        tgt.[Ejer_id] = src.[Ejer_id],
        tgt.[TilladIkkeManuelAngivelse] = src.[TilladIkkeManuelAngivelse],
        tgt.[E1] = src.[E1],
        tgt.[O1] = src.[O1],
        tgt.[ref_RECID] = src.[ref_RECID],
        tgt.[ref_DirPartyTableId] = src.[ref_DirPartyTableId],
        tgt.[ref_DimensionAttributeValueId] = src.[ref_DimensionAttributeValueId],
        tgt.[RunId] = src.[RunId],
        tgt.[StartDateTime] = src.[StartDateTime],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[RECID] = src.[RECID],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [Afdelingsnummer], [AfdelingId], [Afdelingstekst], [AktivFra], [AktivTil], [Suspenderet], [Ejer_id], [TilladIkkeManuelAngivelse], [E1], [O1], [ref_RECID], [ref_DirPartyTableId], [ref_DimensionAttributeValueId], [RunId], [StartDateTime], [EndDateTime], [RdwTs], [CreatedOn], [Modified], [RECID],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[Afdelingsnummer], src.[AfdelingId], src.[Afdelingstekst], src.[AktivFra], src.[AktivTil], src.[Suspenderet], src.[Ejer_id], src.[TilladIkkeManuelAngivelse], src.[E1], src.[O1], src.[ref_RECID], src.[ref_DirPartyTableId], src.[ref_DimensionAttributeValueId], src.[RunId], src.[StartDateTime], src.[EndDateTime], src.[RdwTs], src.[CreatedOn], src.[Modified], src.[RECID],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

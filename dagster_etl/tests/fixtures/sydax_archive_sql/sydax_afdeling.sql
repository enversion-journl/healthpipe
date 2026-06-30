MERGE [arc].[sydax_afdeling] AS tgt
USING [ext].[sydax_afdeling_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[RECID] = src.[RECID] AND tgt.[StartDateTime] = src.[StartDateTime]
WHEN MATCHED THEN UPDATE SET
        tgt.[Afdelingsnummer] = src.[Afdelingsnummer],
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
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [RECID], [StartDateTime], [Afdelingsnummer], [AfdelingId], [Afdelingstekst], [AktivFra], [AktivTil], [Suspenderet], [Ejer_id], [TilladIkkeManuelAngivelse], [E1], [O1], [ref_RECID], [ref_DirPartyTableId], [ref_DimensionAttributeValueId], [RunId], [EndDateTime], [RdwTs], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[RECID], src.[StartDateTime], src.[Afdelingsnummer], src.[AfdelingId], src.[Afdelingstekst], src.[AktivFra], src.[AktivTil], src.[Suspenderet], src.[Ejer_id], src.[TilladIkkeManuelAngivelse], src.[E1], src.[O1], src.[ref_RECID], src.[ref_DirPartyTableId], src.[ref_DimensionAttributeValueId], src.[RunId], src.[EndDateTime], src.[RdwTs], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

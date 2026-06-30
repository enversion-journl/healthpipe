MERGE [arc].[sydax_hovedkonto] AS tgt
USING [ext].[sydax_hovedkonto_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[ref_RECID] = src.[ref_RECID] AND tgt.[StartDateTime] = src.[StartDateTime]
WHEN MATCHED THEN UPDATE SET
        tgt.[HovedkontoId] = src.[HovedkontoId],
        tgt.[Hovedkontonummer] = src.[Hovedkontonummer],
        tgt.[Hovedkontotekst] = src.[Hovedkontotekst],
        tgt.[KontoplanId] = src.[KontoplanId],
        tgt.[HovedkontotypeId] = src.[HovedkontotypeId],
        tgt.[RapporteringstypeId] = src.[RapporteringstypeId],
        tgt.[HovedkontoKategoriId] = src.[HovedkontoKategoriId],
        tgt.[DKForslag] = src.[DKForslag],
        tgt.[DKKrav] = src.[DKKrav],
        tgt.[Saldokontrol] = src.[Saldokontrol],
        tgt.[ModkontoId] = src.[ModkontoId],
        tgt.[PrimokontoId] = src.[PrimokontoId],
        tgt.[SRUKode] = src.[SRUKode],
        tgt.[Afslut] = src.[Afslut],
        tgt.[TvungenReference] = src.[TvungenReference],
        tgt.[CloseType] = src.[CloseType],
        tgt.[Autoriseret] = src.[Autoriseret],
        tgt.[Standardkoncernkonto] = src.[Standardkoncernkonto],
        tgt.[F1] = src.[F1],
        tgt.[F2] = src.[F2],
        tgt.[F3] = src.[F3],
        tgt.[D1] = src.[D1],
        tgt.[G1] = src.[G1],
        tgt.[Valuta] = src.[Valuta],
        tgt.[ValutaValidering] = src.[ValutaValidering],
        tgt.[BrugerId] = src.[BrugerId],
        tgt.[BrugerValidering] = src.[BrugerValidering],
        tgt.[FinansposteringstypeId] = src.[FinansposteringstypeId],
        tgt.[KonteringValidering] = src.[KonteringValidering],
        tgt.[AktivFra] = src.[AktivFra],
        tgt.[AktivTil] = src.[AktivTil],
        tgt.[Suspenderet] = src.[Suspenderet],
        tgt.[Ejer_id] = src.[Ejer_id],
        tgt.[TilladIkkeManuelAngivelse] = src.[TilladIkkeManuelAngivelse],
        tgt.[ref_MainAccountId] = src.[ref_MainAccountId],
        tgt.[ref_DimensionAttributeValueId] = src.[ref_DimensionAttributeValueId],
        tgt.[RunId] = src.[RunId],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [ref_RECID], [StartDateTime], [HovedkontoId], [Hovedkontonummer], [Hovedkontotekst], [KontoplanId], [HovedkontotypeId], [RapporteringstypeId], [HovedkontoKategoriId], [DKForslag], [DKKrav], [Saldokontrol], [ModkontoId], [PrimokontoId], [SRUKode], [Afslut], [TvungenReference], [CloseType], [Autoriseret], [Standardkoncernkonto], [F1], [F2], [F3], [D1], [G1], [Valuta], [ValutaValidering], [BrugerId], [BrugerValidering], [FinansposteringstypeId], [KonteringValidering], [AktivFra], [AktivTil], [Suspenderet], [Ejer_id], [TilladIkkeManuelAngivelse], [ref_MainAccountId], [ref_DimensionAttributeValueId], [RunId], [EndDateTime], [RdwTs], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[ref_RECID], src.[StartDateTime], src.[HovedkontoId], src.[Hovedkontonummer], src.[Hovedkontotekst], src.[KontoplanId], src.[HovedkontotypeId], src.[RapporteringstypeId], src.[HovedkontoKategoriId], src.[DKForslag], src.[DKKrav], src.[Saldokontrol], src.[ModkontoId], src.[PrimokontoId], src.[SRUKode], src.[Afslut], src.[TvungenReference], src.[CloseType], src.[Autoriseret], src.[Standardkoncernkonto], src.[F1], src.[F2], src.[F3], src.[D1], src.[G1], src.[Valuta], src.[ValutaValidering], src.[BrugerId], src.[BrugerValidering], src.[FinansposteringstypeId], src.[KonteringValidering], src.[AktivFra], src.[AktivTil], src.[Suspenderet], src.[Ejer_id], src.[TilladIkkeManuelAngivelse], src.[ref_MainAccountId], src.[ref_DimensionAttributeValueId], src.[RunId], src.[EndDateTime], src.[RdwTs], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

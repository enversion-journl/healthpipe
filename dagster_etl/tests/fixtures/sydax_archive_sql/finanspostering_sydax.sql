MERGE [arc].[finanspostering_sydax] AS tgt
USING [ext].[finanspostering_sydax_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[ref_LedgertransId] = src.[ref_LedgertransId]
WHEN MATCHED THEN UPDATE SET
        tgt.[FinansposteringId] = src.[FinansposteringId],
        tgt.[FinanskontoPrismeId] = src.[FinanskontoPrismeId],
        tgt.[FinanskontoId] = src.[FinanskontoId],
        tgt.[Bogfoeringsdato] = src.[Bogfoeringsdato],
        tgt.[Posteringsbeloeb] = src.[Posteringsbeloeb],
        tgt.[Valutakode] = src.[Valutakode],
        tgt.[PosteringsbeloebValuta] = src.[PosteringsbeloebValuta],
        tgt.[Posteringstekst] = src.[Posteringstekst],
        tgt.[AfdelingPrismeId] = src.[AfdelingPrismeId],
        tgt.[Bilagsnummer] = src.[Bilagsnummer],
        tgt.[ErModpostering] = src.[ErModpostering],
        tgt.[DebetKredit] = src.[DebetKredit],
        tgt.[FinanskladdeId] = src.[FinanskladdeId],
        tgt.[FinanstransaktionstypeId] = src.[FinanstransaktionstypeId],
        tgt.[FinansposteringstypeId] = src.[FinansposteringstypeId],
        tgt.[KontrolleretDato] = src.[KontrolleretDato],
        tgt.[KontrolleretAf] = src.[KontrolleretAf],
        tgt.[Oprettelsestidspunkt] = src.[Oprettelsestidspunkt],
        tgt.[OprettetAf] = src.[OprettetAf],
        tgt.[RunId] = src.[RunId],
        tgt.[StartDateTime] = src.[StartDateTime],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[BilagsnummerID] = src.[BilagsnummerID],
        tgt.[Posteringsmoms] = src.[Posteringsmoms],
        tgt.[PosteringsbeloebInklMoms] = src.[PosteringsbeloebInklMoms],
        tgt.[DokumentId] = src.[DokumentId],
        tgt.[BærerId] = src.[BærerId],
        tgt.[FormålId] = src.[FormålId],
        tgt.[RefusionModtager] = src.[RefusionModtager],
        tgt.[Lager_Kasse] = src.[Lager_Kasse],
        tgt.[Ekspeditionsnummer] = src.[Ekspeditionsnummer],
        tgt.[SDIdentifier] = src.[SDIdentifier],
        tgt.[Primosaldo] = src.[Primosaldo],
        tgt.[DataAreaId] = src.[DataAreaId],
        tgt.[FinansrelationId] = src.[FinansrelationId],
        tgt.[Oplysningspligt] = src.[Oplysningspligt],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[Varemomsgruppe] = src.[Varemomsgruppe],
        tgt.[Bogføringstypetekst] = src.[Bogføringstypetekst],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [ref_LedgertransId], [FinansposteringId], [FinanskontoPrismeId], [FinanskontoId], [Bogfoeringsdato], [Posteringsbeloeb], [Valutakode], [PosteringsbeloebValuta], [Posteringstekst], [AfdelingPrismeId], [Bilagsnummer], [ErModpostering], [DebetKredit], [FinanskladdeId], [FinanstransaktionstypeId], [FinansposteringstypeId], [KontrolleretDato], [KontrolleretAf], [Oprettelsestidspunkt], [OprettetAf], [RunId], [StartDateTime], [EndDateTime], [RdwTs], [BilagsnummerID], [Posteringsmoms], [PosteringsbeloebInklMoms], [DokumentId], [BærerId], [FormålId], [RefusionModtager], [Lager_Kasse], [Ekspeditionsnummer], [SDIdentifier], [Primosaldo], [DataAreaId], [FinansrelationId], [Oplysningspligt], [CreatedOn], [Modified], [Varemomsgruppe], [Bogføringstypetekst],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[ref_LedgertransId], src.[FinansposteringId], src.[FinanskontoPrismeId], src.[FinanskontoId], src.[Bogfoeringsdato], src.[Posteringsbeloeb], src.[Valutakode], src.[PosteringsbeloebValuta], src.[Posteringstekst], src.[AfdelingPrismeId], src.[Bilagsnummer], src.[ErModpostering], src.[DebetKredit], src.[FinanskladdeId], src.[FinanstransaktionstypeId], src.[FinansposteringstypeId], src.[KontrolleretDato], src.[KontrolleretAf], src.[Oprettelsestidspunkt], src.[OprettetAf], src.[RunId], src.[StartDateTime], src.[EndDateTime], src.[RdwTs], src.[BilagsnummerID], src.[Posteringsmoms], src.[PosteringsbeloebInklMoms], src.[DokumentId], src.[BærerId], src.[FormålId], src.[RefusionModtager], src.[Lager_Kasse], src.[Ekspeditionsnummer], src.[SDIdentifier], src.[Primosaldo], src.[DataAreaId], src.[FinansrelationId], src.[Oplysningspligt], src.[CreatedOn], src.[Modified], src.[Varemomsgruppe], src.[Bogføringstypetekst],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

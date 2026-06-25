MERGE [arc].[kreditortransaktion_sydax] AS tgt
USING [ext].[kreditortransaktion_sydax_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[ref_VendTransId] = src.[ref_VendTransId]
WHEN MATCHED THEN UPDATE SET
        tgt.[KreditortransaktionId] = src.[KreditortransaktionId],
        tgt.[KreditorId] = src.[KreditorId],
        tgt.[Transaktionsdato] = src.[Transaktionsdato],
        tgt.[Bilagsnummer] = src.[Bilagsnummer],
        tgt.[Fakturanummer] = src.[Fakturanummer],
        tgt.[EANnummer] = src.[EANnummer],
        tgt.[Fakturadato] = src.[Fakturadato],
        tgt.[Transaktionsbeloeb] = src.[Transaktionsbeloeb],
        tgt.[Valutakode] = src.[Valutakode],
        tgt.[TransaktionsbeloebValuta] = src.[TransaktionsbeloebValuta],
        tgt.[UdlignetBeloeb] = src.[UdlignetBeloeb],
        tgt.[UdlignetBeloebValuta] = src.[UdlignetBeloebValuta],
        tgt.[Transaktionstekst] = src.[Transaktionstekst],
        tgt.[Forfaldsdato] = src.[Forfaldsdato],
        tgt.[ErLukket] = src.[ErLukket],
        tgt.[LukketDato] = src.[LukketDato],
        tgt.[AfdelingPrismeId] = src.[AfdelingPrismeId],
        tgt.[JuridiskEnhed] = src.[JuridiskEnhed],
        tgt.[Bankregistreringsnummer] = src.[Bankregistreringsnummer],
        tgt.[Bankkontonummer] = src.[Bankkontonummer],
        tgt.[ErKorrektion] = src.[ErKorrektion],
        tgt.[KreditortransaktionstypeId] = src.[KreditortransaktionstypeId],
        tgt.[ErGodkendt] = src.[ErGodkendt],
        tgt.[GodkendtDato] = src.[GodkendtDato],
        tgt.[GodkendtAf] = src.[GodkendtAf],
        tgt.[Oprettelsestidspunkt] = src.[Oprettelsestidspunkt],
        tgt.[OprettetAf] = src.[OprettetAf],
        tgt.[IsCounterpost] = src.[IsCounterpost],
        tgt.[CounterpostsThisKreditortransaktionId] = src.[CounterpostsThisKreditortransaktionId],
        tgt.[Betalingstekst] = src.[Betalingstekst],
        tgt.[BetalingReturkode] = src.[BetalingReturkode],
        tgt.[BetalingYderligerInfo] = src.[BetalingYderligerInfo],
        tgt.[Virksomhedsenhed] = src.[Virksomhedsenhed],
        tgt.[VirksomhedsenhedNavn] = src.[VirksomhedsenhedNavn],
        tgt.[RunId] = src.[RunId],
        tgt.[StartDateTime] = src.[StartDateTime],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[SidstUdlignetDato] = src.[SidstUdlignetDato],
        tgt.[SidstUdlignetBilagsnummer] = src.[SidstUdlignetBilagsnummer],
        tgt.[DokumentId] = src.[DokumentId],
        tgt.[DataAreaId] = src.[DataAreaId],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [ref_VendTransId], [KreditortransaktionId], [KreditorId], [Transaktionsdato], [Bilagsnummer], [Fakturanummer], [EANnummer], [Fakturadato], [Transaktionsbeloeb], [Valutakode], [TransaktionsbeloebValuta], [UdlignetBeloeb], [UdlignetBeloebValuta], [Transaktionstekst], [Forfaldsdato], [ErLukket], [LukketDato], [AfdelingPrismeId], [JuridiskEnhed], [Bankregistreringsnummer], [Bankkontonummer], [ErKorrektion], [KreditortransaktionstypeId], [ErGodkendt], [GodkendtDato], [GodkendtAf], [Oprettelsestidspunkt], [OprettetAf], [IsCounterpost], [CounterpostsThisKreditortransaktionId], [Betalingstekst], [BetalingReturkode], [BetalingYderligerInfo], [Virksomhedsenhed], [VirksomhedsenhedNavn], [RunId], [StartDateTime], [EndDateTime], [RdwTs], [SidstUdlignetDato], [SidstUdlignetBilagsnummer], [DokumentId], [DataAreaId], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[ref_VendTransId], src.[KreditortransaktionId], src.[KreditorId], src.[Transaktionsdato], src.[Bilagsnummer], src.[Fakturanummer], src.[EANnummer], src.[Fakturadato], src.[Transaktionsbeloeb], src.[Valutakode], src.[TransaktionsbeloebValuta], src.[UdlignetBeloeb], src.[UdlignetBeloebValuta], src.[Transaktionstekst], src.[Forfaldsdato], src.[ErLukket], src.[LukketDato], src.[AfdelingPrismeId], src.[JuridiskEnhed], src.[Bankregistreringsnummer], src.[Bankkontonummer], src.[ErKorrektion], src.[KreditortransaktionstypeId], src.[ErGodkendt], src.[GodkendtDato], src.[GodkendtAf], src.[Oprettelsestidspunkt], src.[OprettetAf], src.[IsCounterpost], src.[CounterpostsThisKreditortransaktionId], src.[Betalingstekst], src.[BetalingReturkode], src.[BetalingYderligerInfo], src.[Virksomhedsenhed], src.[VirksomhedsenhedNavn], src.[RunId], src.[StartDateTime], src.[EndDateTime], src.[RdwTs], src.[SidstUdlignetDato], src.[SidstUdlignetBilagsnummer], src.[DokumentId], src.[DataAreaId], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

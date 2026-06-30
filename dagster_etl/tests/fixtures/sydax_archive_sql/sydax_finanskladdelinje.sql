MERGE [arc].[sydax_finanskladdelinje] AS tgt
USING [ext].[sydax_finanskladdelinje_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[ref_LedgertransId] = src.[ref_LedgertransId]
WHEN MATCHED THEN UPDATE SET
        tgt.[FinanskladdelinjeId] = src.[FinanskladdelinjeId],
        tgt.[Linjenummer] = src.[Linjenummer],
        tgt.[dato] = src.[dato],
        tgt.[Beskrivelse] = src.[Beskrivelse],
        tgt.[Debet] = src.[Debet],
        tgt.[Kredit] = src.[Kredit],
        tgt.[FinanskladdeId] = src.[FinanskladdeId],
        tgt.[FinanstransaktionstypeId] = src.[FinanstransaktionstypeId],
        tgt.[Ekspeditionsnummer] = src.[Ekspeditionsnummer],
        tgt.[Maskinnummer] = src.[Maskinnummer],
        tgt.[FinanskontoId] = src.[FinanskontoId],
        tgt.[BilagsnummerID] = src.[BilagsnummerID],
        tgt.[DokumentId] = src.[DokumentId],
        tgt.[ref_RecId] = src.[ref_RecId],
        tgt.[RunId] = src.[RunId],
        tgt.[StartDateTime] = src.[StartDateTime],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [ref_LedgertransId], [FinanskladdelinjeId], [Linjenummer], [dato], [Beskrivelse], [Debet], [Kredit], [FinanskladdeId], [FinanstransaktionstypeId], [Ekspeditionsnummer], [Maskinnummer], [FinanskontoId], [BilagsnummerID], [DokumentId], [ref_RecId], [RunId], [StartDateTime], [EndDateTime], [RdwTs], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[ref_LedgertransId], src.[FinanskladdelinjeId], src.[Linjenummer], src.[dato], src.[Beskrivelse], src.[Debet], src.[Kredit], src.[FinanskladdeId], src.[FinanstransaktionstypeId], src.[Ekspeditionsnummer], src.[Maskinnummer], src.[FinanskontoId], src.[BilagsnummerID], src.[DokumentId], src.[ref_RecId], src.[RunId], src.[StartDateTime], src.[EndDateTime], src.[RdwTs], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

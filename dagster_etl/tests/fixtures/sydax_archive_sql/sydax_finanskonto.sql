MERGE [arc].[sydax_finanskonto] AS tgt
USING [ext].[sydax_finanskonto_dfm] AS src
    ON tgt.[Kildesystem] = src.[Kildesystem] AND tgt.[FinanskontoId] = src.[FinanskontoId] AND tgt.[StartDateTime] = src.[StartDateTime]
WHEN MATCHED THEN UPDATE SET
        tgt.[Kontonummer] = src.[Kontonummer],
        tgt.[ErLukket] = src.[ErLukket],
        tgt.[ve_RECID] = src.[ve_RECID],
        tgt.[afd_RECID] = src.[afd_RECID],
        tgt.[os_RECID] = src.[os_RECID],
        tgt.[hk_RECID] = src.[hk_RECID],
        tgt.[ma_RECID] = src.[ma_RECID],
        tgt.[grp_RECID] = src.[grp_RECID],
        tgt.[art_RECID] = src.[art_RECID],
        tgt.[d1_RECID] = src.[d1_RECID],
        tgt.[d2_RECID] = src.[d2_RECID],
        tgt.[l_RECID] = src.[l_RECID],
        tgt.[ref_DIMENSIONATTRIBUTEVALUECOMBINATIONId] = src.[ref_DIMENSIONATTRIBUTEVALUECOMBINATIONId],
        tgt.[RunId] = src.[RunId],
        tgt.[EndDateTime] = src.[EndDateTime],
        tgt.[RdwTs] = src.[RdwTs],
        tgt.[ref_DIMENSIONATTRIBUTEVALUECOMBINATION_RECID] = src.[ref_DIMENSIONATTRIBUTEVALUECOMBINATION_RECID],
        tgt.[CreatedOn] = src.[CreatedOn],
        tgt.[Modified] = src.[Modified],
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        [Kildesystem], [FinanskontoId], [StartDateTime], [Kontonummer], [ErLukket], [ve_RECID], [afd_RECID], [os_RECID], [hk_RECID], [ma_RECID], [grp_RECID], [art_RECID], [d1_RECID], [d2_RECID], [l_RECID], [ref_DIMENSIONATTRIBUTEVALUECOMBINATIONId], [RunId], [EndDateTime], [RdwTs], [ref_DIMENSIONATTRIBUTEVALUECOMBINATION_RECID], [CreatedOn], [Modified],
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        src.[Kildesystem], src.[FinanskontoId], src.[StartDateTime], src.[Kontonummer], src.[ErLukket], src.[ve_RECID], src.[afd_RECID], src.[os_RECID], src.[hk_RECID], src.[ma_RECID], src.[grp_RECID], src.[art_RECID], src.[d1_RECID], src.[d2_RECID], src.[l_RECID], src.[ref_DIMENSIONATTRIBUTEVALUECOMBINATIONId], src.[RunId], src.[EndDateTime], src.[RdwTs], src.[ref_DIMENSIONATTRIBUTEVALUECOMBINATION_RECID], src.[CreatedOn], src.[Modified],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );

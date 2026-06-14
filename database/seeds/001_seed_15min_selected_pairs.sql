START TRANSACTION;

INSERT INTO signal_table_15min (
    uuid,
    asset_1,
    asset_2,
    tp,
    sl,
    rolling_window_15m,
    open_threshold_multiplier,
    entry_z_threshold,
    zscore_sl_threshold,
    enabled,
    last_update_ts
)
VALUES
    ('ARPA_HFT', 'ARPA/USDT:USDT', 'HFT/USDT:USDT', 0.04, 0.03, 300, 1.1, 2.2, 6, 1, NOW(6)),
    ('CYS_POPCAT', 'CYS/USDT:USDT', 'POPCAT/USDT:USDT', 0.04, 0.03, 100, 1.1, 2.2, 6, 1, NOW(6)),
    ('CYS_F', 'CYS/USDT:USDT', 'F/USDT:USDT', 0.04, 0.03, 100, 0.9, 1.8, 6, 1, NOW(6)),
    ('LPT_GMX', 'LPT/USDT:USDT', 'GMX/USDT:USDT', 0.04, 0.04, 300, 0.9, 1.8, 6, 1, NOW(6)),
    ('DASH_PENGU', 'DASH/USDT:USDT', 'PENGU/USDT:USDT', 0.04, 0.04, 200, 0.9, 1.8, 6, 1, NOW(6)),
    ('SNT_CVC', 'SNT/USDT:USDT', 'CVC/USDT:USDT', 0.03, 0.03, 300, 1.0, 2.0, 6, 1, NOW(6)),
    ('ACE_PUFFER', 'ACE/USDT:USDT', 'PUFFER/USDT:USDT', 0.04, 0.04, 100, 1.1, 2.2, 6, 1, NOW(6)),
    ('CHZ_MTL', 'CHZ/USDT:USDT', 'MTL/USDT:USDT', 0.02, 0.02, 300, 1.1, 2.2, 6, 1, NOW(6)),
    ('METIS_ARPA', 'METIS/USDT:USDT', 'ARPA/USDT:USDT', 0.04, 0.04, 200, 1.0, 2.0, 6, 1, NOW(6)),
    ('ARKM_INX', 'ARKM/USDT:USDT', 'INX/USDT:USDT', 0.04, 0.04, 300, 1.0, 2.0, 6, 1, NOW(6)),
    ('POLYX_MTL', 'POLYX/USDT:USDT', 'MTL/USDT:USDT', 0.03, 0.03, 300, 0.9, 1.8, 6, 1, NOW(6)),
    ('XMR_XVS', 'XMR/USDT:USDT', 'XVS/USDT:USDT', 0.04, 0.04, 300, 1.1, 2.2, 6, 1, NOW(6)),
    ('VET_CVC', 'VET/USDT:USDT', 'CVC/USDT:USDT', 0.04, 0.03, 300, 1.1, 2.2, 6, 1, NOW(6)),
    ('AXS_ICX', 'AXS/USDT:USDT', 'ICX/USDT:USDT', 0.03, 0.03, 300, 1.1, 2.2, 6, 1, NOW(6)),
    ('CHZ_STEEM', 'CHZ/USDT:USDT', 'STEEM/USDT:USDT', 0.04, 0.02, 200, 1.0, 2.0, 6, 1, NOW(6)),
    ('ONT_CVC', 'ONT/USDT:USDT', 'CVC/USDT:USDT', 0.03, 0.04, 200, 1.1, 2.2, 6, 1, NOW(6)),
    ('ONT_ICX', 'ONT/USDT:USDT', 'ICX/USDT:USDT', 0.02, 0.03, 300, 1.1, 2.2, 6, 1, NOW(6)),
    ('XVG_SNT', 'XVG/USDT:USDT', 'SNT/USDT:USDT', 0.03, 0.04, 200, 0.9, 1.8, 6, 1, NOW(6)),
    ('ORDI_GMX', 'ORDI/USDT:USDT', 'GMX/USDT:USDT', 0.04, 0.02, 300, 1.0, 2.0, 6, 1, NOW(6)),
    ('LUNA2_ACT', 'LUNA2/USDT:USDT', 'ACT/USDT:USDT', 0.04, 0.04, 300, 1.1, 2.2, 6, 1, NOW(6)),
    ('JUP_BAND', 'JUP/USDT:USDT', 'BAND/USDT:USDT', 0.03, 0.02, 200, 1.0, 2.0, 6, 1, NOW(6)),
    ('XTZ_SNT', 'XTZ/USDT:USDT', 'SNT/USDT:USDT', 0.03, 0.04, 200, 1.1, 2.2, 6, 1, NOW(6)),
    ('ONT_STEEM', 'ONT/USDT:USDT', 'STEEM/USDT:USDT', 0.03, 0.04, 300, 0.9, 1.8, 6, 1, NOW(6)),
    ('ANKR_WOO', 'ANKR/USDT:USDT', 'WOO/USDT:USDT', 0.03, 0.03, 300, 1.0, 2.0, 6, 1, NOW(6)),
    ('FLR_CVC', 'FLR/USDT:USDT', 'CVC/USDT:USDT', 0.03, 0.03, 200, 1.1, 2.2, 6, 1, NOW(6)),
    ('ATOM_10000SATS', 'ATOM/USDT:USDT', '10000SATS/USDT:USDT', 0.02, 0.03, 100, 1.0, 2.0, 6, 1, NOW(6)),
    ('ARK_CVC', 'ARK/USDT:USDT', 'CVC/USDT:USDT', 0.02, 0.04, 300, 1.1, 2.2, 6, 1, NOW(6)),
    ('SAND_CVC', 'SAND/USDT:USDT', 'CVC/USDT:USDT', 0.03, 0.03, 200, 1.1, 2.2, 6, 1, NOW(6)),
    ('NEO_ANKR', 'NEO/USDT:USDT', 'ANKR/USDT:USDT', 0.04, 0.03, 200, 1.1, 2.2, 6, 1, NOW(6))
ON DUPLICATE KEY UPDATE
    asset_1 = VALUES(asset_1),
    asset_2 = VALUES(asset_2),
    tp = VALUES(tp),
    sl = VALUES(sl),
    rolling_window_15m = VALUES(rolling_window_15m),
    open_threshold_multiplier = VALUES(open_threshold_multiplier),
    entry_z_threshold = VALUES(entry_z_threshold),
    zscore_sl_threshold = VALUES(zscore_sl_threshold),
    enabled = VALUES(enabled);

INSERT INTO pair_state_15min (uuid, hl_timeframe_minutes, last_update_ts)
SELECT uuid, 60, NOW(6)
FROM signal_table_15min
WHERE uuid IN (
    'ARPA_HFT', 'CYS_POPCAT', 'CYS_F', 'LPT_GMX', 'DASH_PENGU',
    'SNT_CVC', 'ACE_PUFFER', 'CHZ_MTL', 'METIS_ARPA', 'ARKM_INX',
    'POLYX_MTL', 'XMR_XVS', 'VET_CVC', 'AXS_ICX', 'CHZ_STEEM',
    'ONT_CVC', 'ONT_ICX', 'XVG_SNT', 'ORDI_GMX', 'LUNA2_ACT',
    'JUP_BAND', 'XTZ_SNT', 'ONT_STEEM', 'ANKR_WOO', 'FLR_CVC',
    'ATOM_10000SATS', 'ARK_CVC', 'SAND_CVC', 'NEO_ANKR'
)
ON DUPLICATE KEY UPDATE uuid = VALUES(uuid);

COMMIT;

SELECT COUNT(*) AS selected_signal_rows
FROM signal_table_15min
WHERE enabled = 1;

SELECT COUNT(*) AS selected_state_rows
FROM pair_state_15min
WHERE uuid IN (
    'ARPA_HFT', 'CYS_POPCAT', 'CYS_F', 'LPT_GMX', 'DASH_PENGU',
    'SNT_CVC', 'ACE_PUFFER', 'CHZ_MTL', 'METIS_ARPA', 'ARKM_INX',
    'POLYX_MTL', 'XMR_XVS', 'VET_CVC', 'AXS_ICX', 'CHZ_STEEM',
    'ONT_CVC', 'ONT_ICX', 'XVG_SNT', 'ORDI_GMX', 'LUNA2_ACT',
    'JUP_BAND', 'XTZ_SNT', 'ONT_STEEM', 'ANKR_WOO', 'FLR_CVC',
    'ATOM_10000SATS', 'ARK_CVC', 'SAND_CVC', 'NEO_ANKR'
);

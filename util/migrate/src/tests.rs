use crate::migrate::Migrate;
use ckb_app_config::DBConfig;
use ckb_chain_spec::consensus::build_genesis_epoch_ext;
use ckb_db::RocksDB;
use ckb_db_schema::{
    COLUMN_BLOCK_BODY, COLUMN_BLOCK_EPOCH, COLUMN_BLOCK_EXT, COLUMN_BLOCK_HEADER,
    COLUMN_BLOCK_PROPOSAL_IDS, COLUMN_BLOCK_UNCLE, COLUMN_EPOCH, COLUMN_HASH_INDEX, COLUMN_INDEX,
    COLUMN_META, COLUMN_NUMBER_HASH, COLUMNS, META_CURRENT_EPOCH_KEY, META_TIP_HEADER_KEY,
    MIGRATION_VERSION_KEY,
};
use ckb_systemtime::unix_time_as_millis;
use ckb_types::{
    core::{
        BlockBuilder, BlockExt, Capacity, TransactionBuilder, capacity_bytes, hardfork::HardForks,
    },
    packed::{self, Bytes},
    prelude::*,
    utilities::DIFF_TWO,
};

#[test]
fn test_mock_migration() {
    let tmp_dir = tempfile::Builder::new()
        .prefix("test_mock_migration")
        .tempdir()
        .unwrap();
    let config = DBConfig {
        path: tmp_dir.as_ref().to_path_buf(),
        ..Default::default()
    };
    // 0.25-0.34 ckb's columns is 12
    let db = RocksDB::open(&config, 12);
    let cellbase = TransactionBuilder::default()
        .witness(Bytes::default())
        .build();
    let epoch_ext =
        build_genesis_epoch_ext(capacity_bytes!(100), DIFF_TWO, 1_000, 4 * 60 * 60, (1, 40));
    let genesis = BlockBuilder::default().transaction(cellbase).build();

    // genesis block insert is copy from 0.34 ckb
    let db_txn = db.transaction();

    // insert block
    {
        let hash = genesis.hash();
        let header: packed::HeaderView = genesis.header().into();
        let number = header.data().raw().number();
        let uncles: packed::UncleBlockVecView = genesis.uncles().into();
        let proposals = genesis.data().proposals();
        db_txn
            .put(COLUMN_INDEX, number.as_slice(), hash.as_slice())
            .unwrap();
        db_txn
            .put(COLUMN_BLOCK_HEADER, hash.as_slice(), header.as_slice())
            .unwrap();
        db_txn
            .put(COLUMN_BLOCK_UNCLE, hash.as_slice(), uncles.as_slice())
            .unwrap();
        db_txn
            .put(
                COLUMN_BLOCK_PROPOSAL_IDS,
                hash.as_slice(),
                proposals.as_slice(),
            )
            .unwrap();
        for (index, tx) in genesis.transactions().into_iter().enumerate() {
            let key = packed::TransactionKey::new_builder()
                .block_hash(hash.clone())
                .index(index)
                .build();
            let tx_data = Into::<packed::TransactionView>::into(tx);
            db_txn
                .put(COLUMN_BLOCK_BODY, key.as_slice(), tx_data.as_slice())
                .unwrap();
        }
    }

    let ext = BlockExt {
        received_at: unix_time_as_millis(),
        total_difficulty: genesis.difficulty(),
        total_uncles_count: 0,
        verified: None,
        txs_fees: vec![],
        cycles: None,
        txs_sizes: None,
    };

    // insert_block_epoch_index
    {
        db_txn
            .put(
                COLUMN_BLOCK_EPOCH,
                genesis.header().hash().as_slice(),
                epoch_ext.last_block_hash_in_previous_epoch().as_slice(),
            )
            .unwrap()
    }
    // insert epoch ext
    {
        db_txn
            .put(
                COLUMN_EPOCH,
                epoch_ext.last_block_hash_in_previous_epoch().as_slice(),
                Into::<packed::EpochExt>::into(&epoch_ext).as_slice(),
            )
            .unwrap();
        let epoch_number: packed::Uint64 = epoch_ext.number().into();
        db_txn
            .put(
                COLUMN_EPOCH,
                epoch_number.as_slice(),
                epoch_ext.last_block_hash_in_previous_epoch().as_slice(),
            )
            .unwrap()
    }

    // insert tip header
    {
        db_txn
            .put(
                COLUMN_META,
                META_TIP_HEADER_KEY,
                genesis.header().hash().as_slice(),
            )
            .unwrap()
    }

    // insert block ext
    {
        db_txn
            .put(
                COLUMN_BLOCK_EXT,
                genesis.header().hash().as_slice(),
                Into::<packed::BlockExtV1>::into(ext).as_slice(),
            )
            .unwrap()
    }

    // insert_current_epoch_ext
    {
        db_txn
            .put(
                COLUMN_META,
                META_CURRENT_EPOCH_KEY,
                Into::<packed::EpochExt>::into(epoch_ext).as_slice(),
            )
            .unwrap()
    }

    db_txn.commit().unwrap();

    drop(db_txn);
    drop(db);

    let mg = Migrate::new(tmp_dir.as_ref().to_path_buf(), HardForks::new_mirana());

    let db = mg.open_bulk_load_db().unwrap().unwrap();

    mg.migrate(db, false).unwrap();

    let mg2 = Migrate::new(tmp_dir.as_ref().to_path_buf(), HardForks::new_mirana());

    let rdb = mg2.open_read_only_db().unwrap().unwrap();

    assert_eq!(mg2.check(&rdb, true), std::cmp::Ordering::Equal)
}

#[test]
fn test_refactor_rocksdb_schema_migration() {
    let tmp_dir = tempfile::Builder::new()
        .prefix("test_refactor_rocksdb_schema_migration")
        .tempdir()
        .unwrap();
    let config = DBConfig {
        path: tmp_dir.as_ref().to_path_buf(),
        ..Default::default()
    };
    let db = RocksDB::open(&config, COLUMNS);

    let cellbase = TransactionBuilder::default()
        .witness(Bytes::default())
        .build();
    let epoch_ext =
        build_genesis_epoch_ext(capacity_bytes!(100), DIFF_TWO, 1_000, 4 * 60 * 60, (1, 40));
    let genesis = BlockBuilder::default().transaction(cellbase).build();
    let genesis_hash = genesis.hash();
    let number = genesis.number();
    let number_packed: packed::Uint64 = number.into();

    let db_txn = db.transaction();

    // old schema: COLUMN_INDEX stores both number->hash and hash->number
    db_txn
        .put(
            COLUMN_INDEX,
            number_packed.as_slice(),
            genesis_hash.as_slice(),
        )
        .unwrap();
    db_txn
        .put(
            COLUMN_INDEX,
            genesis_hash.as_slice(),
            number_packed.as_slice(),
        )
        .unwrap();

    // old schema: block columns keyed by block_hash
    let header: packed::HeaderView = genesis.header().into();
    let uncles: packed::UncleBlockVecView = genesis.uncles().into();
    let proposals = genesis.data().proposals();
    db_txn
        .put(
            COLUMN_BLOCK_HEADER,
            genesis_hash.as_slice(),
            header.as_slice(),
        )
        .unwrap();
    db_txn
        .put(
            COLUMN_BLOCK_UNCLE,
            genesis_hash.as_slice(),
            uncles.as_slice(),
        )
        .unwrap();
    db_txn
        .put(
            COLUMN_BLOCK_PROPOSAL_IDS,
            genesis_hash.as_slice(),
            proposals.as_slice(),
        )
        .unwrap();
    db_txn
        .put(
            COLUMN_BLOCK_EPOCH,
            genesis_hash.as_slice(),
            epoch_ext.last_block_hash_in_previous_epoch().as_slice(),
        )
        .unwrap();
    db_txn
        .put(
            COLUMN_BLOCK_EXT,
            genesis_hash.as_slice(),
            Into::<packed::BlockExtV1>::into(BlockExt {
                received_at: unix_time_as_millis(),
                total_difficulty: genesis.difficulty(),
                total_uncles_count: 0,
                verified: Some(true),
                txs_fees: vec![],
                cycles: Some(vec![]),
                txs_sizes: Some(vec![]),
            })
            .as_slice(),
        )
        .unwrap();

    for (index, tx) in genesis.transactions().into_iter().enumerate() {
        let old_tx_key = packed::TransactionKey::new_builder()
            .block_hash(genesis_hash.clone())
            .index(index)
            .build();
        let tx_data = Into::<packed::TransactionView>::into(tx);
        db_txn
            .put(COLUMN_BLOCK_BODY, old_tx_key.as_slice(), tx_data.as_slice())
            .unwrap();
    }

    let number_hash_key = packed::NumberHash::new_builder()
        .number(number)
        .block_hash(genesis_hash.clone())
        .build();
    let txs_len: packed::Uint32 = (genesis.transactions().len() as u32).into();
    db_txn
        .put(
            COLUMN_NUMBER_HASH,
            number_hash_key.as_slice(),
            txs_len.as_slice(),
        )
        .unwrap();

    db_txn
        .put(
            COLUMN_META,
            META_TIP_HEADER_KEY,
            genesis.header().hash().as_slice(),
        )
        .unwrap();
    db_txn
        .put(
            COLUMN_META,
            META_CURRENT_EPOCH_KEY,
            Into::<packed::EpochExt>::into(epoch_ext).as_slice(),
        )
        .unwrap();

    db_txn.commit().unwrap();

    // emulate pre-schema-refactor latest migration version
    db.put_default(MIGRATION_VERSION_KEY, "20231101000000")
        .unwrap();

    let mg = Migrate::new(tmp_dir.as_ref().to_path_buf(), HardForks::new_mirana());
    let migrated_db = mg.migrate(db, false).unwrap();

    let block_key = genesis_hash.to_block_key(number);
    let tx_key = genesis_hash.to_tx_key(number, 0);
    let old_tx_key = packed::TransactionKey::new_builder()
        .block_hash(genesis_hash.clone())
        .index(0)
        .build();

    assert!(
        migrated_db
            .get_pinned(COLUMN_BLOCK_HEADER, &block_key)
            .unwrap()
            .is_some()
    );
    assert!(
        migrated_db
            .get_pinned(COLUMN_BLOCK_HEADER, genesis_hash.as_slice())
            .unwrap()
            .is_none()
    );

    assert!(
        migrated_db
            .get_pinned(COLUMN_BLOCK_BODY, &tx_key)
            .unwrap()
            .is_some()
    );
    assert!(
        migrated_db
            .get_pinned(COLUMN_BLOCK_BODY, old_tx_key.as_slice())
            .unwrap()
            .is_none()
    );

    let hash_index_value = migrated_db
        .get_pinned(COLUMN_HASH_INDEX, genesis_hash.as_slice())
        .unwrap()
        .expect("COLUMN_HASH_INDEX must be set");
    assert_eq!(
        packed::Byte32::number_from_index_value(hash_index_value.as_ref()),
        Some(number)
    );
    assert_eq!(
        packed::Byte32::is_main_chain_from_index_value(hash_index_value.as_ref()),
        Some(true)
    );

    let index_value = migrated_db
        .get_pinned(COLUMN_INDEX, number_packed.as_slice())
        .unwrap()
        .expect("number->hash mapping should remain");
    assert_eq!(index_value.as_ref(), genesis_hash.as_slice());
    assert!(
        migrated_db
            .get_pinned(COLUMN_INDEX, genesis_hash.as_slice())
            .unwrap()
            .is_none()
    );
}

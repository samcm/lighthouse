use beacon_chain::blob_verification::GossipVerifiedBlob;
use beacon_chain::custody_context::NodeCustodyType;
use beacon_chain::data_column_verification::{
    CustodyDataColumn, GossipVerifiedDataColumn, KzgVerifiedCustodyDataColumn,
};
use beacon_chain::test_utils::{BeaconChainHarness, generate_data_column_sidecars_from_block};
use eth2::types::{EventKind, SseBlobSidecar, SseDataColumnSidecar};
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::sync::Arc;
use tokio::sync::broadcast::error::TryRecvError;
use types::blob_sidecar::FixedBlobSidecarList;
use types::test_utils::TestRandom;
use types::{BlobSidecar, BlockImportSource, DataColumnSidecar, EthSpec, ForkName, MinimalEthSpec, Slot};

type E = MinimalEthSpec;

/// Verifies that a blob event is emitted when a gossip verified blob is received via gossip or the publish block API.
#[tokio::test]
async fn blob_sidecar_event_on_process_gossip_blob() {
    let spec = Arc::new(ForkName::Deneb.make_genesis_spec(E::default_spec()));
    let harness = BeaconChainHarness::builder(E::default())
        .spec(spec)
        .deterministic_keypairs(8)
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build();

    // subscribe to blob sidecar events
    let event_handler = harness.chain.event_handler.as_ref().unwrap();
    let mut blob_event_receiver = event_handler.subscribe_blob_sidecar();

    // build and process a gossip verified blob
    let kzg = harness.chain.kzg.as_ref();
    let mut rng = StdRng::seed_from_u64(0xDEADBEEF0BAD5EEDu64);
    let sidecar = BlobSidecar::random_valid(&mut rng, kzg)
        .map(Arc::new)
        .unwrap();
    let gossip_verified_blob = GossipVerifiedBlob::__assumed_valid(sidecar);
    let expected_sse_blobs = SseBlobSidecar::from_blob_sidecar(gossip_verified_blob.as_blob());

    let _ = harness
        .chain
        .process_gossip_blob(gossip_verified_blob)
        .await
        .unwrap();

    let sidecar_event = blob_event_receiver.try_recv().unwrap();
    assert_eq!(sidecar_event, EventKind::BlobSidecar(expected_sse_blobs));
}

/// Verifies that a data column event is emitted when a gossip verified data column is received via gossip or the publish block API.
#[tokio::test]
async fn data_column_sidecar_event_on_process_gossip_data_column() {
    let spec = Arc::new(ForkName::Fulu.make_genesis_spec(E::default_spec()));
    let harness = BeaconChainHarness::builder(E::default())
        .spec(spec)
        .deterministic_keypairs(8)
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build();

    // subscribe to blob sidecar events
    let event_handler = harness.chain.event_handler.as_ref().unwrap();
    let mut data_column_event_receiver = event_handler.subscribe_data_column_sidecar();

    // build and process a gossip verified data column
    let mut rng = StdRng::seed_from_u64(0xDEADBEEF0BAD5EEDu64);
    let sidecar = {
        // DA checker only accepts sampling columns, so we need to create one with a sampling index.
        let mut random_sidecar = DataColumnSidecar::random_for_test(&mut rng);
        let slot = Slot::new(10);
        let epoch = slot.epoch(E::slots_per_epoch());
        random_sidecar.signed_block_header.message.slot = slot;
        random_sidecar.index = harness.chain.sampling_columns_for_epoch(epoch)[0];
        random_sidecar
    };
    let gossip_verified_data_column =
        GossipVerifiedDataColumn::__new_for_testing(Arc::new(sidecar));
    let expected_sse_data_column = SseDataColumnSidecar::from_data_column_sidecar(
        gossip_verified_data_column.as_data_column(),
    );

    let _ = harness
        .chain
        .process_gossip_data_columns(vec![gossip_verified_data_column], || Ok(()))
        .await
        .unwrap();

    let sidecar_event = data_column_event_receiver.try_recv().unwrap();
    assert_eq!(
        sidecar_event,
        EventKind::DataColumnSidecar(expected_sse_data_column)
    );
}

/// Verifies that a blob event is emitted when blobs are received via RPC.
#[tokio::test]
async fn blob_sidecar_event_on_process_rpc_blobs() {
    let spec = Arc::new(ForkName::Deneb.make_genesis_spec(E::default_spec()));
    let harness = BeaconChainHarness::builder(E::default())
        .spec(spec)
        .deterministic_keypairs(8)
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build();

    // subscribe to blob sidecar events
    let event_handler = harness.chain.event_handler.as_ref().unwrap();
    let mut blob_event_receiver = event_handler.subscribe_blob_sidecar();

    // build and process multiple rpc blobs
    harness.execution_block_generator().set_min_blob_count(2);

    let head_state = harness.get_current_state();
    let slot = head_state.slot() + 1;
    let ((signed_block, opt_blobs), _) = harness.make_block(head_state, slot).await;
    let (kzg_proofs, blobs) = opt_blobs.unwrap();
    assert!(blobs.len() > 2);

    let blob_1 =
        Arc::new(BlobSidecar::new(0, blobs[0].clone(), &signed_block, kzg_proofs[0]).unwrap());
    let blob_2 =
        Arc::new(BlobSidecar::new(1, blobs[1].clone(), &signed_block, kzg_proofs[1]).unwrap());

    let blobs = FixedBlobSidecarList::new(vec![Some(blob_1.clone()), Some(blob_2.clone())]);
    let expected_sse_blobs = vec![
        SseBlobSidecar::from_blob_sidecar(blob_1.as_ref()),
        SseBlobSidecar::from_blob_sidecar(blob_2.as_ref()),
    ];

    let _ = harness
        .chain
        .process_rpc_blobs(slot, blob_1.block_root(), blobs)
        .await
        .unwrap();

    let mut sse_blobs: Vec<SseBlobSidecar> = vec![];
    while let Ok(sidecar_event) = blob_event_receiver.try_recv() {
        if let EventKind::BlobSidecar(sse_blob_sidecar) = sidecar_event {
            sse_blobs.push(sse_blob_sidecar);
        } else {
            panic!("`BlobSidecar` event kind expected.");
        }
    }
    assert_eq!(sse_blobs, expected_sse_blobs);
}

#[tokio::test]
async fn data_column_sidecar_event_on_process_rpc_columns() {
    let spec = Arc::new(ForkName::Fulu.make_genesis_spec(E::default_spec()));
    let harness = BeaconChainHarness::builder(E::default())
        .spec(spec.clone())
        .deterministic_keypairs(8)
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build();

    // subscribe to blob sidecar events
    let event_handler = harness.chain.event_handler.as_ref().unwrap();
    let mut data_column_event_receiver = event_handler.subscribe_data_column_sidecar();

    // build a valid block
    harness.execution_block_generator().set_min_blob_count(1);

    let head_state = harness.get_current_state();
    let slot = head_state.slot() + 1;
    let ((signed_block, opt_blobs), _) = harness.make_block(head_state, slot).await;
    let (_, blobs) = opt_blobs.unwrap();
    assert!(!blobs.is_empty());

    // load the precomputed column sidecar to avoid computing them for every block in the tests.
    let data_column_sidecars =
        generate_data_column_sidecars_from_block(&signed_block, &harness.chain.spec);
    let sidecar = data_column_sidecars[0].clone();
    let expected_sse_data_column = SseDataColumnSidecar::from_data_column_sidecar(&sidecar);

    let _ = harness
        .chain
        .process_rpc_custody_columns(vec![sidecar])
        .await
        .unwrap();

    let sidecar_event = data_column_event_receiver.try_recv().unwrap();
    assert_eq!(
        sidecar_event,
        EventKind::DataColumnSidecar(expected_sse_data_column)
    );
}

/// Verifies that data column events are emitted when columns are reconstructed.
#[tokio::test]
async fn data_column_sidecar_event_on_reconstruction() {
    let spec = Arc::new(ForkName::Fulu.make_genesis_spec(E::default_spec()));
    let harness = BeaconChainHarness::builder(E::default())
        .spec(spec.clone())
        .deterministic_keypairs(8)
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .node_custody_type(NodeCustodyType::Supernode)
        .build();

    let event_handler = harness.chain.event_handler.as_ref().unwrap();
    let mut data_column_event_receiver = event_handler.subscribe_data_column_sidecar();

    // Build a block with blobs and generate data columns
    harness.execution_block_generator().set_min_blob_count(1);
    let head_state = harness.get_current_state();
    let slot = head_state.slot() + 1;
    let ((signed_block, _), _) = harness.make_block(head_state, slot).await;
    let all_data_columns = generate_data_column_sidecars_from_block(&signed_block, &spec);
    let block_root = signed_block.canonical_root();

    // Add block to DA checker (required for reconstruction)
    harness
        .chain
        .data_availability_checker
        .put_pre_execution_block(block_root, signed_block, BlockImportSource::Gossip)
        .unwrap();

    // Add 50% of columns (indices 0-63) to trigger reconstruction threshold
    let columns_to_add: Vec<_> = all_data_columns
        .iter()
        .take(E::number_of_columns() / 2)
        .cloned()
        .collect();
    let added_indices: std::collections::HashSet<u64> =
        columns_to_add.iter().map(|c| c.index).collect();

    let kzg = harness.chain.kzg.as_ref();
    let verified_columns: Vec<_> = columns_to_add
        .into_iter()
        .map(|sidecar| {
            KzgVerifiedCustodyDataColumn::new(CustodyDataColumn::from_asserted_custody(sidecar), kzg)
                .unwrap()
        })
        .collect();

    harness
        .chain
        .data_availability_checker
        .put_kzg_verified_custody_data_columns(block_root, verified_columns)
        .unwrap();

    // Clear any prior events
    while data_column_event_receiver.try_recv().is_ok() {}

    // Trigger reconstruction
    let (_, reconstructed) = harness
        .chain
        .reconstruct_data_columns(block_root)
        .await
        .unwrap()
        .expect("reconstruction should succeed");
    assert!(!reconstructed.is_empty());

    // Collect SSE events (channel may overflow, so just verify we get some)
    let mut received_indices = vec![];
    loop {
        match data_column_event_receiver.try_recv() {
            Ok(EventKind::DataColumnSidecar(sse)) => received_indices.push(sse.index),
            Ok(_) => panic!("unexpected event type"),
            Err(TryRecvError::Lagged(_)) => continue,
            Err(_) => break,
        }
    }

    // Verify events are for reconstructed columns, not the originally-added ones
    assert!(!received_indices.is_empty(), "should receive SSE events");
    for idx in &received_indices {
        assert!(!added_indices.contains(idx), "event for original column {idx}");
    }
}

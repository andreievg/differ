 pub(crate) async fn pull(
        &self,
        connection: &StorageConnection,
    ) -> Result<(), CentralSyncError> {
        use CentralSyncError as Error;
        let central_sync_cursor = CentralSyncPullCursor::new(&connection);
        let cursor: u32 = central_sync_cursor.get_cursor().unwrap_or_else(|_| {
            info!("Initialising new central sync cursor...");
            0
        });

        // Arbitrary batch size.
        const BATCH_SIZE: u32 = 500;

        loop {
            info!("Pulling central sync records...");
            let sync_batch: CentralSyncBatchV5 = self
                .sync_api_v5
                .get_central_records(cursor, BATCH_SIZE)
                .await
                .map_err(Error::PullCentralSyncRecordsError)?;

            let batch_length = sync_batch.data.len();
            info!(
                "Inserting {} central sync records into central sync buffer",
                batch_length
            );

            for sync_record in sync_batch.data {
                let buffer_row = sync_record
                    .to_sync_buffer_row()
                    .map_err(Error::PullCentralTranslateToSyncBuffer)?;
                insert_one_and_update_cursor(connection, &buffer_row, sync_record.id)
                    .map_err(Error::UpdateCentralSyncBufferRecordsError)?;
            }

            if batch_length == 0 {
                info!("Central sync buffer is up-to-date");
                break;
            }
        }
        Ok(())
    }

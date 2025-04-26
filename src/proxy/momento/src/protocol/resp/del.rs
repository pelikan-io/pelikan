use crate::klog::*;
use crate::*;
use protocol_resp::*;
use std::io::Write;

use super::update_method_metrics;

pub async fn del(
    client: &mut CacheClient,
    cache_name: &str,
    response_buf: &mut Vec<u8>,
    req: &Del,
) -> ProxyResult {
    let keys: Vec<&[u8]> = req.keys().iter().map(|k| &**k).collect();

    for key in keys {
        let client = client.clone();

        update_method_metrics(&DEL, &DEL_EX, async move {
            match timeout(Duration::from_millis(200), client.delete(cache_name, key)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    klog_1(&"hdel", &key, Status::ServerError, 0);
                    return Err(ProxyError::from(e));
                }
                Err(e) => {
                    klog_1(&"hdel", &key, Status::Timeout, 0);
                    return Err(ProxyError::from(e));
                }
            }

            Ok(())
        })
        .await?;
    }

    // NOTE: the Momento protocol does not inform us of how many keys are
    // deleted. We lie to the client and say that they all were deleted.
    write!(response_buf, ":{}\r\n", req.keys().len())?;

    for key in req.keys() {
        klog_1(&"del", &key, Status::Deleted, 0);
    }

    Ok(())
}

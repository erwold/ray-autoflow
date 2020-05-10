#include "flow_prober.h"
#include "config/streaming_config.h"
#include "status.h"

namespace ray {
namespace streaming {


FlowProber::FlowProber()
{
    STREAMING_LOG(INFO) << "[LPQINFO] Scheduler Initialize probe channel ";
    probe_channel_info_.channel_id = ObjectID::FromBinary("probe_channel1234567");
    probe_channel_info_.actor_id = ActorID::FromBinary("prober");
    probe_channel_info_.last_queue_item_delay = 0;
    probe_channel_info_.last_queue_item_latency = 0;
    probe_channel_info_.last_queue_target_diff = 0;
    probe_channel_info_.get_queue_item_times = 0;
    transfer_config_ = std::make_shared<Config>();
    probe_channel_ = std::make_shared<StreamingQueueProber>(transfer_config_, 
                                                        probe_channel_info_);
    probe_channel_->CreateTransferChannel();
    STREAMING_LOG(INFO) << "[LPQINFO] Initialize probe channel "
                        << probe_channel_info_.channel_id 
                        << " actor_id " << probe_channel_info_.actor_id;
}

StreamingStatus FlowProber::Probe(uint8_t *&data, uint32_t &data_size)
{
    uint64_t seq_id;
    STREAMING_LOG(INFO) << "[LPQINFO] try get probe message";
    auto status = probe_channel_->ConsumeItemFromChannel(
                                seq_id, data, data_size, 10);
    probe_channel_info_.get_queue_item_times++;
    if (!data) {
      STREAMING_LOG(INFO) << "[LPQINFO] read probe channel status " << status
                           << " get item timeout ";
      return StreamingStatus::GetBundleTimeOut;
    }
    return StreamingStatus::OK;
}

} // namespace ray
} // namespace streaming

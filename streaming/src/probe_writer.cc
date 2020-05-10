#include "probe_writer.h"

namespace ray {
namespace streaming {

ProbeWriter::ProbeWriter(const ActorID &scheduler_id) {
    STREAMING_LOG(INFO) << "[LPQINFO] Initialize state probe writer";
    scheduler_id_ = scheduler_id;
    ObjectID queue_id = ObjectID::FromBinary("probe_channel1234567");
    probe_channel_ = std::make_shared<StreamingMigrateChannel>(queue_id, scheduler_id_);
}

StreamingStatus ProbeWriter::WriteProbeMessage(uint8_t *data, uint32_t data_size)
{
    STREAMING_LOG(INFO) << "[LPQINFO] Start send probe data";
    auto status = probe_channel_->ProduceItemToChannel(scheduler_id_, data, data_size);
    if (status == StreamingStatus::OK) {
        STREAMING_LOG(INFO) << "[LPQINFO] probe data has been send to" << scheduler_id_;
    }
    return StreamingStatus::OK;
}

} // namespace ray
} // namespace streaming

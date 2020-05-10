#include "state_migrater.h"

namespace ray {
namespace streaming {

StateMigrater::StateMigrater(const ActorID &actor_id) {
    STREAMING_LOG(INFO) << "[LPQINFO] Initialize state migrater";
    actor_id_ = actor_id;
    ObjectID queue_id = ObjectID::FromBinary("migrate_channel12345");
    migrate_channel_ = std::make_shared<StreamingMigrateChannel>(queue_id, actor_id);
}

StreamingStatus StateMigrater::WriteMigrationMessage(const ActorID &actor_id, 
                                            uint8_t *data, uint32_t data_size)
{
    STREAMING_LOG(INFO) << "[LPQINFO] Start send migration data";
    auto status = migrate_channel_->ProduceItemToChannel(actor_id, data, data_size);
    if (status == StreamingStatus::OK) {
        STREAMING_LOG(INFO) << "[LPQINFO] migration data has been send to" << actor_id;
    }
    return StreamingStatus::OK;
}

} // namespace ray
} // namespace streaming

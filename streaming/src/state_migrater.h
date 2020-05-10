#ifndef RAY_STATE_MIGRATER_H
#define RAY_STATE_MIGRATER_H

//#include <memory>

#include "channel.h"
#include "status.h"

namespace ray {
namespace streaming {

class StateMigrater {
public:
    StateMigrater(const ActorID &actor_id);
    StreamingStatus WriteMigrationMessage(const ActorID &peer_actor_id,
                            uint8_t *data, uint32_t data_size);
private:
    ActorID actor_id_;
    std::shared_ptr<StreamingMigrateChannel> migrate_channel_;
};
} // namespace streaming
} // namespace ray
#endif

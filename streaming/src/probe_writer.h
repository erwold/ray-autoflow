#ifndef RAY_PROBE_WRITER_H
#define RAY_PROBE_WRITER_H

#include "channel.h"
#include "status.h"

namespace ray {
namespace streaming {

class ProbeWriter {
public:
    ProbeWriter(const ActorID &scheduler_id);
    StreamingStatus WriteProbeMessage(uint8_t *data, uint32_t data_size);
private:
    ActorID scheduler_id_;
    std::shared_ptr<StreamingMigrateChannel> probe_channel_;
};
} // namespace streaming
} // namespace ray
#endif

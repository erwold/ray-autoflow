#ifndef RAY_FLOW_PROBER_H
#define RAY_FLOW_PROBER_H

//#include <memory>

#include "channel.h"
#include "status.h"
#include "config/streaming_config.h"

namespace ray {
namespace streaming {

class FlowProber {
public:
    FlowProber();
    StreamingStatus Probe(uint8_t *&data, uint32_t &data_size);
private:
    ConsumerChannelInfo probe_channel_info_;
    std::shared_ptr<ConsumerChannel> probe_channel_;
    std::shared_ptr<Config> transfer_config_;
};
} // namespace streaming
} // namespace ray
#endif

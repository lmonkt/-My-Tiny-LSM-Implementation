#pragma once

#include <string>

namespace toni_lsm {

void init_spdlog_file();
void reset_log_level(const std::string &level);

} // namespace toni_lsm
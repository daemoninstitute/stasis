/*
 * Copyright 2025 James Wilson
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

#include <stasis/stasis.hpp>

namespace {

void print_error(stasis::AppError error) {
  std::cerr << "Error: ";
  switch (error) {
  case stasis::AppError::KeyNotFound:
    std::cerr << "Key not found.";
    break;
  case stasis::AppError::NoActiveTransaction:
    std::cerr << "No active transaction.";
    break;
  }
  std::cerr << '\n';
}

enum class CliError : std::uint8_t {
  UnknownCommand,
  InvalidArguments,
};

void print_error(CliError error) {
  std::cerr << "Error: ";
  switch (error) {
  case CliError::UnknownCommand:
    std::cerr << "Unknown command.";
    break;
  case CliError::InvalidArguments:
    std::cerr << "Invalid arguments for command.";
    break;
  }
  std::cerr << '\n';
}

class CliProcessor {
public:
  void process_set(std::stringstream &args) {
    std::string key;
    std::string value;
    std::string extra;
    if (!(args >> key >> value) || (args >> extra)) {
      print_error(CliError::InvalidArguments);
    } else {
      if (auto result = store_.handle_set({key}, {value}); !result) {
        print_error(result.error());
      }
    }
  }

  void process_get(std::stringstream &args) {
    std::string key;
    std::string extra;
    if (!(args >> key) || (args >> extra)) {
      print_error(CliError::InvalidArguments);
    } else {
      if (auto result = store_.handle_get(key)) {
        std::cout << *result << '\n';
      } else {
        print_error(result.error());
      }
    }
  }

  void process_delete(std::stringstream &args) {
    std::string key;
    std::string extra;
    if (!(args >> key) || (args >> extra)) {
      print_error(CliError::InvalidArguments);
    } else {
      if (auto result = store_.handle_delete(key); !result) {
        print_error(result.error());
      }
    }
  }

  void process_begin(std::stringstream &args) {
    std::string extra;
    if (args >> extra) {
      print_error(CliError::InvalidArguments);
    } else {
      if (auto result = store_.handle_begin(); !result) {
        print_error(result.error());
      }
    }
  }

  void process_commit(std::stringstream &args) {
    std::string extra;
    if (args >> extra) {
      print_error(CliError::InvalidArguments);
    } else {
      if (auto result = store_.handle_commit(); !result) {
        print_error(result.error());
      }
    }
  }

  void process_rollback(std::stringstream &args) {
    std::string extra;
    if (args >> extra) {
      print_error(CliError::InvalidArguments);
    } else {
      if (auto result = store_.handle_rollback(); !result) {
        print_error(result.error());
      }
    }
  }

private:
  stasis::KeyValueStore store_;
};

} // namespace

auto main() -> int {
  CliProcessor processor;

  const std::unordered_map<std::string,
                           void (CliProcessor::*)(std::stringstream &)>
      command_handlers{
          {"SET", &CliProcessor::process_set},
          {"GET", &CliProcessor::process_get},
          {"DELETE", &CliProcessor::process_delete},
          {"BEGIN", &CliProcessor::process_begin},
          {"COMMIT", &CliProcessor::process_commit},
          {"ROLLBACK", &CliProcessor::process_rollback},
      };

  std::string line;
  while (true) {
    std::cout << "> " << std::flush;
    if (!std::getline(std::cin, line)) {
      break;
    }

    std::stringstream input_stream(line);
    std::string command;
    input_stream >> command;

    if (command.empty()) {
      continue;
    }

    if (command == "QUIT") {
      break;
    }

    if (const auto handler_iterator = command_handlers.find(command);
        handler_iterator != command_handlers.end()) {
      auto handler_ptr = handler_iterator->second;
      (processor.*handler_ptr)(input_stream);
    } else {
      print_error(CliError::UnknownCommand);
    }
  }

  return 0;
}

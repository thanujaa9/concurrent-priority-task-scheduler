CXX ?= c++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -Wpedantic -pthread -Iinclude
SAN_FLAGS ?= -std=c++17 -g -Wall -Wextra -Wpedantic -pthread -Iinclude
BUILD_DIR := build

.PHONY: all test benchmark tsan asan clean

all: $(BUILD_DIR)/benchmark $(BUILD_DIR)/scheduler_tests

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/benchmark: src/task_scheduler.cpp src/benchmark.cpp include/task_scheduler.hpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) src/task_scheduler.cpp src/benchmark.cpp -o $@

$(BUILD_DIR)/scheduler_tests: src/task_scheduler.cpp tests/scheduler_tests.cpp include/task_scheduler.hpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) src/task_scheduler.cpp tests/scheduler_tests.cpp -o $@

test: $(BUILD_DIR)/scheduler_tests
	./$(BUILD_DIR)/scheduler_tests

benchmark: $(BUILD_DIR)/benchmark
	./$(BUILD_DIR)/benchmark

tsan: | $(BUILD_DIR)
	$(CXX) $(SAN_FLAGS) -fsanitize=thread src/task_scheduler.cpp tests/scheduler_tests.cpp -o $(BUILD_DIR)/tsan_tests
	./$(BUILD_DIR)/tsan_tests

asan: | $(BUILD_DIR)
	$(CXX) $(SAN_FLAGS) -fsanitize=address,undefined src/task_scheduler.cpp tests/scheduler_tests.cpp -o $(BUILD_DIR)/asan_tests
	./$(BUILD_DIR)/asan_tests

clean:
	rm -rf $(BUILD_DIR)

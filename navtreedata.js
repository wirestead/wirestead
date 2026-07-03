/*
 @licstart  The following is the entire license notice for the JavaScript code in this file.

 The MIT License (MIT)

 Copyright (C) 1997-2020 by Dimitri van Heesch

 Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 and associated documentation files (the "Software"), to deal in the Software without restriction,
 including without limitation the rights to use, copy, modify, merge, publish, distribute,
 sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in all copies or
 substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 @licend  The above is the entire license notice for the JavaScript code in this file
*/
var NAVTREE =
[
  [ "unilink", "index.html", [
    [ "Unilink Documentation", "index.html", "index" ],
    [ "unilink Logo Assets", "md__2home_2runner_2work_2unilink-docs_2unilink-docs_2docs_2assets_2logo_2README.html", [
      [ "Files", "md__2home_2runner_2work_2unilink-docs_2unilink-docs_2docs_2assets_2logo_2README.html#autotoc_md1", null ],
      [ "Usage", "md__2home_2runner_2work_2unilink-docs_2unilink-docs_2docs_2assets_2logo_2README.html#autotoc_md2", null ]
    ] ],
    [ "Design Notes", "contrib_design.html", null ],
    [ "SendResult API Design", "contrib_design_send_result.html", [
      [ "Problem Statement", "contrib_design_send_result.html#autotoc_md336", null ],
      [ "Current bool API", "contrib_design_send_result.html#autotoc_md337", null ],
      [ "Goals", "contrib_design_send_result.html#autotoc_md338", null ],
      [ "Non-Goals", "contrib_design_send_result.html#autotoc_md339", null ],
      [ "Proposed API", "contrib_design_send_result.html#autotoc_md340", [
        [ "Type Definitions", "contrib_design_send_result.html#autotoc_md341", null ],
        [ "API Names", "contrib_design_send_result.html#autotoc_md342", null ]
      ] ],
      [ "SendStatus Semantics", "contrib_design_send_result.html#autotoc_md343", null ],
      [ "Reliable Semantics", "contrib_design_send_result.html#autotoc_md344", null ],
      [ "BestEffort Semantics", "contrib_design_send_result.html#autotoc_md345", null ],
      [ "Relationship To RuntimeStats", "contrib_design_send_result.html#autotoc_md346", null ],
      [ "API Compatibility", "contrib_design_send_result.html#autotoc_md347", null ],
      [ "Server Send Results", "contrib_design_send_result.html#autotoc_md348", null ],
      [ "Migration Plan", "contrib_design_send_result.html#autotoc_md349", [
        [ "Phase 1: Design", "contrib_design_send_result.html#autotoc_md350", null ],
        [ "Phase 2: Experimental API", "contrib_design_send_result.html#autotoc_md351", null ],
        [ "Phase 3: Official 0.8 API", "contrib_design_send_result.html#autotoc_md352", null ],
        [ "Phase 4: Long-Term", "contrib_design_send_result.html#autotoc_md353", null ]
      ] ],
      [ "Open Questions", "contrib_design_send_result.html#autotoc_md354", null ]
    ] ],
    [ "Contributor Guide", "contrib_index.html", [
      [ "Building the Library", "contrib_index.html#autotoc_md361", null ],
      [ "Architecture", "contrib_index.html#autotoc_md363", null ],
      [ "Design Notes", "contrib_index.html#autotoc_md365", null ],
      [ "Quick Links (User Docs)", "contrib_index.html#autotoc_md367", null ],
      [ "Build Guide", "contrib_build.html", [
        [ "Table of Contents", "contrib_build.html#autotoc_md259", null ],
        [ "Quick Build", "contrib_build.html#autotoc_md261", [
          [ "Basic Build (Recommended)", "contrib_build.html#autotoc_md262", null ]
        ] ],
        [ "Important Build Notes", "contrib_build.html#autotoc_md264", null ],
        [ "Build Configurations", "contrib_build.html#autotoc_md266", [
          [ "Minimal Build (without Configuration Management API)", "contrib_build.html#autotoc_md267", null ],
          [ "Full Build (includes Configuration Management API)", "contrib_build.html#autotoc_md269", null ]
        ] ],
        [ "Build Options Reference", "contrib_build.html#autotoc_md271", [
          [ "Core Options", "contrib_build.html#autotoc_md272", null ],
          [ "Development Options", "contrib_build.html#autotoc_md273", null ],
          [ "Installation Options", "contrib_build.html#autotoc_md274", null ]
        ] ],
        [ "Build Types Comparison", "contrib_build.html#autotoc_md276", [
          [ "Release Build (Default)", "contrib_build.html#autotoc_md277", null ],
          [ "Debug Build", "contrib_build.html#autotoc_md279", null ],
          [ "RelWithDebInfo Build", "contrib_build.html#autotoc_md281", null ]
        ] ],
        [ "Advanced Build Examples", "contrib_build.html#autotoc_md283", [
          [ "Example 1: Minimal Production Build", "contrib_build.html#autotoc_md284", null ],
          [ "Example 2: Development Build", "contrib_build.html#autotoc_md286", null ],
          [ "Example 3: Testing with Sanitizers", "contrib_build.html#autotoc_md288", null ],
          [ "Example 4: Build with Custom Boost Location", "contrib_build.html#autotoc_md290", null ],
          [ "Example 5: Build with Specific Compiler", "contrib_build.html#autotoc_md292", null ]
        ] ],
        [ "Platform-Specific Builds", "contrib_build.html#autotoc_md294", [
          [ "Ubuntu 22.04 (Recommended)", "contrib_build.html#autotoc_md295", null ],
          [ "Ubuntu 20.04 Build", "contrib_build.html#autotoc_md297", [
            [ "Prerequisites", "contrib_build.html#autotoc_md298", null ],
            [ "Build Steps", "contrib_build.html#autotoc_md299", null ],
            [ "Notes", "contrib_build.html#autotoc_md300", null ]
          ] ],
          [ "Debian 11+", "contrib_build.html#autotoc_md302", null ],
          [ "Fedora 35+", "contrib_build.html#autotoc_md304", null ],
          [ "Arch Linux", "contrib_build.html#autotoc_md306", null ]
        ] ],
        [ "Build Performance Tips", "contrib_build.html#autotoc_md308", [
          [ "Parallel Builds", "contrib_build.html#autotoc_md309", null ],
          [ "Ccache for Faster Rebuilds", "contrib_build.html#autotoc_md310", null ],
          [ "Ninja Build System (Faster than Make)", "contrib_build.html#autotoc_md311", null ]
        ] ],
        [ "Installation", "contrib_build.html#autotoc_md313", [
          [ "System-Wide Installation", "contrib_build.html#autotoc_md314", null ],
          [ "Custom Installation Directory", "contrib_build.html#autotoc_md315", null ],
          [ "Uninstall", "contrib_build.html#autotoc_md316", null ]
        ] ],
        [ "Verifying the Build", "contrib_build.html#autotoc_md318", [
          [ "Run Unit Tests", "contrib_build.html#autotoc_md319", null ],
          [ "Run Focused Tests", "contrib_build.html#autotoc_md320", null ],
          [ "Check Library Symbols", "contrib_build.html#autotoc_md321", null ]
        ] ],
        [ "Troubleshooting", "contrib_build.html#autotoc_md323", [
          [ "Problem: CMake Can't Find Boost", "contrib_build.html#autotoc_md324", null ],
          [ "Problem: Compiler Not Found", "contrib_build.html#autotoc_md325", null ],
          [ "Problem: Out of Memory During Build", "contrib_build.html#autotoc_md326", null ],
          [ "Problem: Permission Denied During Install", "contrib_build.html#autotoc_md327", null ]
        ] ],
        [ "CMake Package Integration", "contrib_build.html#autotoc_md329", [
          [ "Using the Installed Package", "contrib_build.html#autotoc_md330", null ],
          [ "Custom Installation Prefix", "contrib_build.html#autotoc_md331", null ],
          [ "Package Components", "contrib_build.html#autotoc_md332", null ],
          [ "Verification", "contrib_build.html#autotoc_md333", null ]
        ] ],
        [ "Next Steps", "contrib_build.html#autotoc_md335", null ]
      ] ],
      [ "Testing Guide", "contrib_testing.html", [
        [ "Table of Contents", "contrib_testing.html#autotoc_md410", null ],
        [ "Quick Start", "contrib_testing.html#autotoc_md412", [
          [ "Build and Run All Tests", "contrib_testing.html#autotoc_md413", null ],
          [ "Windows Build & Test Workflow", "contrib_testing.html#autotoc_md415", null ]
        ] ],
        [ "Running Tests", "contrib_testing.html#autotoc_md417", [
          [ "Run All Tests", "contrib_testing.html#autotoc_md418", null ],
          [ "Run Specific Test Categories", "contrib_testing.html#autotoc_md420", null ],
          [ "Run Tests with Verbose Output", "contrib_testing.html#autotoc_md422", null ],
          [ "Run Tests in Parallel", "contrib_testing.html#autotoc_md424", null ]
        ] ],
        [ "UDP-specific test policies", "contrib_testing.html#autotoc_md426", null ],
        [ "Test Categories", "contrib_testing.html#autotoc_md427", [
          [ "Core Tests", "contrib_testing.html#autotoc_md428", null ],
          [ "Memory Safety Tests", "contrib_testing.html#autotoc_md430", null ],
          [ "Concurrency Safety Tests", "contrib_testing.html#autotoc_md432", null ],
          [ "Benchmarking", "contrib_testing.html#autotoc_md435", null ],
          [ "Stress Tests", "contrib_testing.html#autotoc_md437", null ]
        ] ],
        [ "Memory Safety Validation", "contrib_testing.html#autotoc_md439", [
          [ "Built-in Memory Tracking", "contrib_testing.html#autotoc_md440", null ],
          [ "AddressSanitizer (ASan)", "contrib_testing.html#autotoc_md442", null ],
          [ "ThreadSanitizer (TSan)", "contrib_testing.html#autotoc_md444", null ],
          [ "Valgrind", "contrib_testing.html#autotoc_md446", null ]
        ] ],
        [ "Continuous Integration", "contrib_testing.html#autotoc_md448", [
          [ "GitHub Actions Integration", "contrib_testing.html#autotoc_md449", null ],
          [ "CI/CD Build Matrix", "contrib_testing.html#autotoc_md451", null ],
          [ "Ubuntu 20.04 Support", "contrib_testing.html#autotoc_md453", null ],
          [ "View CI/CD Results", "contrib_testing.html#autotoc_md455", null ]
        ] ],
        [ "Writing Custom Tests", "contrib_testing.html#autotoc_md457", [
          [ "Test Structure", "contrib_testing.html#autotoc_md458", null ],
          [ "Example: Custom Integration Test", "contrib_testing.html#autotoc_md460", null ],
          [ "Running Custom Tests", "contrib_testing.html#autotoc_md462", null ]
        ] ],
        [ "Test Configuration", "contrib_testing.html#autotoc_md464", [
          [ "CTest Configuration", "contrib_testing.html#autotoc_md465", null ],
          [ "Environment Variables", "contrib_testing.html#autotoc_md467", null ]
        ] ],
        [ "Troubleshooting Tests", "contrib_testing.html#autotoc_md469", [
          [ "Test Failures", "contrib_testing.html#autotoc_md470", null ],
          [ "Port Conflicts", "contrib_testing.html#autotoc_md472", null ],
          [ "Memory Issues", "contrib_testing.html#autotoc_md474", null ]
        ] ],
        [ "Performance Regression Testing", "contrib_testing.html#autotoc_md476", null ],
        [ "Code Coverage", "contrib_testing.html#autotoc_md478", [
          [ "Generate Coverage Report", "contrib_testing.html#autotoc_md479", null ],
          [ "View HTML Coverage Report", "contrib_testing.html#autotoc_md480", null ]
        ] ],
        [ "Next Steps", "contrib_testing.html#autotoc_md482", null ]
      ] ],
      [ "Implementation Status", "contrib_impl_status.html", [
        [ "Scope", "contrib_impl_status.html#autotoc_md355", null ],
        [ "C++ API Surface", "contrib_impl_status.html#autotoc_md356", null ],
        [ "Python Binding Scope", "contrib_impl_status.html#autotoc_md357", null ],
        [ "Build And Test Status", "contrib_impl_status.html#autotoc_md358", null ],
        [ "Recommended Reading Order", "contrib_impl_status.html#autotoc_md359", null ]
      ] ],
      [ "Test Structure", "contrib_test_structure.html", [
        [ "Layout", "contrib_test_structure.html#autotoc_md399", null ],
        [ "What Each Area Covers", "contrib_test_structure.html#autotoc_md400", null ],
        [ "Build-Time Controls", "contrib_test_structure.html#autotoc_md401", null ],
        [ "Running Tests", "contrib_test_structure.html#autotoc_md402", [
          [ "Run All Registered Tests", "contrib_test_structure.html#autotoc_md403", null ],
          [ "Run By Broad Category", "contrib_test_structure.html#autotoc_md404", null ],
          [ "Useful Focused Runs", "contrib_test_structure.html#autotoc_md405", null ],
          [ "Inspect What Is Currently Registered", "contrib_test_structure.html#autotoc_md406", null ]
        ] ],
        [ "Notes", "contrib_test_structure.html#autotoc_md407", null ],
        [ "CI/CD Integration", "contrib_test_structure.html#autotoc_md408", null ]
      ] ],
      [ "Unilink System Architecture", "contrib_arch.html", [
        [ "Table of Contents", "contrib_arch.html#autotoc_md98", null ],
        [ "Overview", "contrib_arch.html#autotoc_md100", [
          [ "Design Goals", "contrib_arch.html#autotoc_md101", null ]
        ] ],
        [ "Layered Architecture", "contrib_arch.html#autotoc_md103", [
          [ "Layer Responsibilities", "contrib_arch.html#autotoc_md104", [
            [ "1. Builder API Layer", "contrib_arch.html#autotoc_md105", null ],
            [ "2. Wrapper API Layer", "contrib_arch.html#autotoc_md106", null ],
            [ "3. Transport Layer", "contrib_arch.html#autotoc_md107", null ],
            [ "4. Common Utilities Layer", "contrib_arch.html#autotoc_md108", null ]
          ] ]
        ] ],
        [ "Core Components", "contrib_arch.html#autotoc_md110", [
          [ "1. Builder System", "contrib_arch.html#autotoc_md111", null ],
          [ "2. Wrapper System", "contrib_arch.html#autotoc_md112", null ],
          [ "3. Transport System", "contrib_arch.html#autotoc_md113", null ],
          [ "4. Common Utilities", "contrib_arch.html#autotoc_md114", null ]
        ] ],
        [ "Design Patterns", "contrib_arch.html#autotoc_md116", [
          [ "1. Builder Pattern", "contrib_arch.html#autotoc_md117", null ],
          [ "2. Dependency Injection", "contrib_arch.html#autotoc_md118", null ],
          [ "3. Observer Pattern", "contrib_arch.html#autotoc_md119", null ],
          [ "4. Singleton Pattern", "contrib_arch.html#autotoc_md120", null ],
          [ "5. RAII (Resource Acquisition Is Initialization)", "contrib_arch.html#autotoc_md121", null ],
          [ "6. Template Method Pattern", "contrib_arch.html#autotoc_md122", null ]
        ] ],
        [ "Threading Model", "contrib_arch.html#autotoc_md124", [
          [ "Overview", "contrib_arch.html#autotoc_md125", null ],
          [ "Thread Safety Model", "contrib_arch.html#autotoc_md126", [
            [ "1. Runtime Methods", "contrib_arch.html#autotoc_md127", null ],
            [ "2. Callbacks", "contrib_arch.html#autotoc_md128", null ],
            [ "3. Shared State", "contrib_arch.html#autotoc_md129", null ]
          ] ],
          [ "IO Context Management", "contrib_arch.html#autotoc_md130", [
            [ "Shared Context (Default)", "contrib_arch.html#autotoc_md131", null ],
            [ "Independent Context", "contrib_arch.html#autotoc_md132", null ]
          ] ]
        ] ],
        [ "Memory Management", "contrib_arch.html#autotoc_md134", [
          [ "1. Smart Pointers", "contrib_arch.html#autotoc_md135", null ],
          [ "2. Memory Pool", "contrib_arch.html#autotoc_md136", null ],
          [ "3. Memory Tracking", "contrib_arch.html#autotoc_md137", null ],
          [ "4. Safe Data Buffer", "contrib_arch.html#autotoc_md138", null ]
        ] ],
        [ "Error Handling", "contrib_arch.html#autotoc_md140", [
          [ "Error Propagation Flow", "contrib_arch.html#autotoc_md141", null ],
          [ "Error Categories", "contrib_arch.html#autotoc_md142", null ],
          [ "Error Recovery Strategies", "contrib_arch.html#autotoc_md143", [
            [ "Automatic Retry", "contrib_arch.html#autotoc_md144", null ]
          ] ]
        ] ],
        [ "Configuration System", "contrib_arch.html#autotoc_md146", [
          [ "Compile-Time Configuration", "contrib_arch.html#autotoc_md147", null ],
          [ "Runtime Configuration", "contrib_arch.html#autotoc_md148", null ]
        ] ],
        [ "Performance Considerations", "contrib_arch.html#autotoc_md150", [
          [ "1. Asynchronous I/O", "contrib_arch.html#autotoc_md151", null ],
          [ "2. Zero-Copy Operations", "contrib_arch.html#autotoc_md152", null ],
          [ "3. Memory Pooling", "contrib_arch.html#autotoc_md153", null ]
        ] ],
        [ "Extension Points", "contrib_arch.html#autotoc_md155", [
          [ "1. Custom Transports", "contrib_arch.html#autotoc_md156", null ],
          [ "2. Custom Builders", "contrib_arch.html#autotoc_md157", null ],
          [ "3. Custom Error Handlers", "contrib_arch.html#autotoc_md158", null ]
        ] ],
        [ "Development & Tooling", "contrib_arch.html#autotoc_md160", [
          [ "Documentation Generation", "contrib_arch.html#autotoc_md161", null ]
        ] ],
        [ "Testing Architecture", "contrib_arch.html#autotoc_md163", [
          [ "1. Dependency Injection", "contrib_arch.html#autotoc_md164", null ],
          [ "2. Independent Contexts", "contrib_arch.html#autotoc_md165", null ],
          [ "3. State Verification", "contrib_arch.html#autotoc_md166", null ]
        ] ],
        [ "Summary", "contrib_arch.html#autotoc_md168", null ],
        [ "Runtime Behavior Model", "contrib_arch_runtime.html", [
          [ "Table of Contents", "contrib_arch_runtime.html#autotoc_md171", null ],
          [ "Threading Model & Callback Execution", "contrib_arch_runtime.html#autotoc_md173", [
            [ "Architecture Diagram", "contrib_arch_runtime.html#autotoc_md174", null ],
            [ "Key Points", "contrib_arch_runtime.html#autotoc_md176", [
              [ "Concurrent Runtime Methods", "contrib_arch_runtime.html#autotoc_md177", null ],
              [ "✅ Callback Execution Context", "contrib_arch_runtime.html#autotoc_md179", null ],
              [ "⚠️ Never Block in Callbacks", "contrib_arch_runtime.html#autotoc_md181", null ],
              [ "✅ Thread-Safe State Access", "contrib_arch_runtime.html#autotoc_md183", null ]
            ] ],
            [ "Threading Model Summary", "contrib_arch_runtime.html#autotoc_md185", null ]
          ] ],
          [ "Reconnection Policy & State Machine", "contrib_arch_runtime.html#autotoc_md187", [
            [ "State Machine Diagram", "contrib_arch_runtime.html#autotoc_md188", null ],
            [ "Connection States", "contrib_arch_runtime.html#autotoc_md190", null ],
            [ "Configuration Example", "contrib_arch_runtime.html#autotoc_md192", null ],
            [ "Retry Behavior", "contrib_arch_runtime.html#autotoc_md194", [
              [ "Default Behavior", "contrib_arch_runtime.html#autotoc_md195", null ],
              [ "Retry Interval Configuration", "contrib_arch_runtime.html#autotoc_md196", null ],
              [ "State Callbacks", "contrib_arch_runtime.html#autotoc_md198", null ],
              [ "Manual Control", "contrib_arch_runtime.html#autotoc_md200", null ]
            ] ],
            [ "Reconnection Best Practices", "contrib_arch_runtime.html#autotoc_md202", [
              [ "1. Choose Appropriate Retry Interval", "contrib_arch_runtime.html#autotoc_md203", null ],
              [ "2. Handle State Transitions", "contrib_arch_runtime.html#autotoc_md205", null ],
              [ "3. Graceful Shutdown", "contrib_arch_runtime.html#autotoc_md207", null ]
            ] ]
          ] ],
          [ "Backpressure Handling", "contrib_arch_runtime.html#autotoc_md209", [
            [ "Backpressure Flow", "contrib_arch_runtime.html#autotoc_md210", null ],
            [ "Backpressure Configuration", "contrib_arch_runtime.html#autotoc_md212", null ],
            [ "Backpressure Strategies", "contrib_arch_runtime.html#autotoc_md214", [
              [ "Strategy 1: Pause Sending", "contrib_arch_runtime.html#autotoc_md215", null ],
              [ "Strategy 2: Rate Limiting", "contrib_arch_runtime.html#autotoc_md217", null ],
              [ "Strategy 3: Drop Data", "contrib_arch_runtime.html#autotoc_md219", null ]
            ] ],
            [ "Backpressure Monitoring", "contrib_arch_runtime.html#autotoc_md221", null ],
            [ "Memory Safety", "contrib_arch_runtime.html#autotoc_md223", null ]
          ] ],
          [ "Best Practices", "contrib_arch_runtime.html#autotoc_md225", [
            [ "1. Threading Best Practices", "contrib_arch_runtime.html#autotoc_md226", [
              [ "✅ DO", "contrib_arch_runtime.html#autotoc_md227", null ],
              [ "❌ DON'T", "contrib_arch_runtime.html#autotoc_md228", null ]
            ] ],
            [ "2. Reconnection Best Practices", "contrib_arch_runtime.html#autotoc_md230", [
              [ "✅ DO", "contrib_arch_runtime.html#autotoc_md231", null ],
              [ "❌ DON'T", "contrib_arch_runtime.html#autotoc_md232", null ]
            ] ],
            [ "3. Backpressure Best Practices", "contrib_arch_runtime.html#autotoc_md234", [
              [ "✅ DO", "contrib_arch_runtime.html#autotoc_md235", null ],
              [ "❌ DON'T", "contrib_arch_runtime.html#autotoc_md236", null ]
            ] ]
          ] ],
          [ "Performance Considerations", "contrib_arch_runtime.html#autotoc_md238", [
            [ "Threading Overhead", "contrib_arch_runtime.html#autotoc_md239", null ],
            [ "Reconnection Overhead", "contrib_arch_runtime.html#autotoc_md240", null ],
            [ "Backpressure Overhead", "contrib_arch_runtime.html#autotoc_md241", null ]
          ] ],
          [ "Next Steps", "contrib_arch_runtime.html#autotoc_md243", null ]
        ] ],
        [ "Memory Safety Architecture", "contrib_arch_memory.html", [
          [ "Table of Contents", "contrib_arch_memory.html#autotoc_md10", null ],
          [ "Overview", "contrib_arch_memory.html#autotoc_md12", [
            [ "Memory Safety Model", "contrib_arch_memory.html#autotoc_md13", null ],
            [ "Safety Levels", "contrib_arch_memory.html#autotoc_md15", null ]
          ] ],
          [ "Safe Data Handling", "contrib_arch_memory.html#autotoc_md17", [
            [ "SafeDataBuffer", "contrib_arch_memory.html#autotoc_md18", null ],
            [ "Features", "contrib_arch_memory.html#autotoc_md20", [
              [ "1. Construction Validation", "contrib_arch_memory.html#autotoc_md21", null ],
              [ "2. Safe Type Conversions", "contrib_arch_memory.html#autotoc_md23", null ],
              [ "3. Memory Validation", "contrib_arch_memory.html#autotoc_md25", null ]
            ] ],
            [ "Safe Span", "contrib_arch_memory.html#autotoc_md27", null ]
          ] ],
          [ "Thread-Safe State Management", "contrib_arch_memory.html#autotoc_md29", [
            [ "ThreadSafeState", "contrib_arch_memory.html#autotoc_md30", null ],
            [ "AtomicState", "contrib_arch_memory.html#autotoc_md32", null ],
            [ "ThreadSafeCounter", "contrib_arch_memory.html#autotoc_md34", null ],
            [ "ThreadSafeFlag", "contrib_arch_memory.html#autotoc_md36", null ],
            [ "Thread Safety Summary", "contrib_arch_memory.html#autotoc_md38", null ]
          ] ],
          [ "Memory Tracking", "contrib_arch_memory.html#autotoc_md40", [
            [ "Built-in Memory Tracking", "contrib_arch_memory.html#autotoc_md41", null ],
            [ "Features", "contrib_arch_memory.html#autotoc_md43", [
              [ "1. Allocation Tracking", "contrib_arch_memory.html#autotoc_md44", null ],
              [ "2. Leak Detection", "contrib_arch_memory.html#autotoc_md46", null ],
              [ "3. Performance Monitoring", "contrib_arch_memory.html#autotoc_md48", null ],
              [ "4. Debug Reports", "contrib_arch_memory.html#autotoc_md50", null ]
            ] ],
            [ "Zero Overhead in Release", "contrib_arch_memory.html#autotoc_md52", null ]
          ] ],
          [ "AddressSanitizer Support", "contrib_arch_memory.html#autotoc_md54", [
            [ "Enable AddressSanitizer", "contrib_arch_memory.html#autotoc_md55", null ],
            [ "What ASan Detects", "contrib_arch_memory.html#autotoc_md57", null ],
            [ "Running with ASan", "contrib_arch_memory.html#autotoc_md59", null ],
            [ "Performance Impact", "contrib_arch_memory.html#autotoc_md61", null ]
          ] ],
          [ "Best Practices", "contrib_arch_memory.html#autotoc_md63", [
            [ "1. Buffer Management", "contrib_arch_memory.html#autotoc_md64", [
              [ "✅ DO", "contrib_arch_memory.html#autotoc_md65", null ],
              [ "❌ DON'T", "contrib_arch_memory.html#autotoc_md66", null ]
            ] ],
            [ "2. Type Conversions", "contrib_arch_memory.html#autotoc_md68", [
              [ "✅ DO", "contrib_arch_memory.html#autotoc_md69", null ],
              [ "❌ DON'T", "contrib_arch_memory.html#autotoc_md70", null ]
            ] ],
            [ "3. Thread Safety", "contrib_arch_memory.html#autotoc_md72", [
              [ "✅ DO", "contrib_arch_memory.html#autotoc_md73", null ],
              [ "❌ DON'T", "contrib_arch_memory.html#autotoc_md74", null ]
            ] ],
            [ "4. Memory Tracking", "contrib_arch_memory.html#autotoc_md76", [
              [ "✅ DO", "contrib_arch_memory.html#autotoc_md77", null ],
              [ "❌ DON'T", "contrib_arch_memory.html#autotoc_md78", null ]
            ] ],
            [ "5. Sanitizers", "contrib_arch_memory.html#autotoc_md80", [
              [ "✅ DO", "contrib_arch_memory.html#autotoc_md81", null ],
              [ "❌ DON'T", "contrib_arch_memory.html#autotoc_md82", null ]
            ] ]
          ] ],
          [ "Memory Safety Benefits", "contrib_arch_memory.html#autotoc_md84", [
            [ "Prevents Common Vulnerabilities", "contrib_arch_memory.html#autotoc_md85", null ],
            [ "Performance", "contrib_arch_memory.html#autotoc_md87", null ]
          ] ],
          [ "Testing Memory Safety", "contrib_arch_memory.html#autotoc_md89", [
            [ "Unit Tests", "contrib_arch_memory.html#autotoc_md90", null ],
            [ "Integration Tests", "contrib_arch_memory.html#autotoc_md92", null ],
            [ "Continuous Integration", "contrib_arch_memory.html#autotoc_md94", null ]
          ] ],
          [ "Next Steps", "contrib_arch_memory.html#autotoc_md96", null ]
        ] ],
        [ "Transport Channel Contract", "contrib_arch_channel.html", [
          [ "1. Introduction", "contrib_arch_channel.html#autotoc_md3", null ],
          [ "2. Core Principles", "contrib_arch_channel.html#autotoc_md4", null ],
          [ "3. Stop Semantics: No Callbacks After Stop()", "contrib_arch_channel.html#autotoc_md5", null ],
          [ "4. Backpressure Policy", "contrib_arch_channel.html#autotoc_md6", null ],
          [ "5. Error Handling Consistency", "contrib_arch_channel.html#autotoc_md7", null ],
          [ "6. State Transitions", "contrib_arch_channel.html#autotoc_md8", null ]
        ] ],
        [ "Wrapper Contract", "contrib_arch_wrapper.html", [
          [ "Scope", "contrib_arch_wrapper.html#autotoc_md244", null ],
          [ "Core Rules", "contrib_arch_wrapper.html#autotoc_md245", [
            [ "1. <tt>start()</tt> reflects real transport state", "contrib_arch_wrapper.html#autotoc_md246", null ],
            [ "2. Repeated <tt>start()</tt> and <tt>stop()</tt> are safe", "contrib_arch_wrapper.html#autotoc_md247", null ],
            [ "3. <tt>auto_start(true)</tt> follows the same startup contract", "contrib_arch_wrapper.html#autotoc_md248", null ]
          ] ],
          [ "External <tt>io_context</tt> Contract", "contrib_arch_wrapper.html#autotoc_md249", [
            [ "4. Externally supplied <tt>io_context</tt> can be reused", "contrib_arch_wrapper.html#autotoc_md250", null ],
            [ "5. Managed and unmanaged external contexts have different ownership rules", "contrib_arch_wrapper.html#autotoc_md251", null ]
          ] ],
          [ "Callback Contract", "contrib_arch_wrapper.html#autotoc_md252", [
            [ "6. Handler replacement uses the latest callback", "contrib_arch_wrapper.html#autotoc_md253", null ],
            [ "7. No wrapper callbacks after <tt>stop()</tt> returns", "contrib_arch_wrapper.html#autotoc_md254", null ],
            [ "8. Generic fallback errors are normalized", "contrib_arch_wrapper.html#autotoc_md255", null ]
          ] ],
          [ "Transport-Agnostic Expectations", "contrib_arch_wrapper.html#autotoc_md256", null ],
          [ "Testing Status", "contrib_arch_wrapper.html#autotoc_md257", null ]
        ] ]
      ] ]
    ] ],
    [ "Orin Nano Validation", "contrib_orin_nano_validation.html", [
      [ "Scope", "contrib_orin_nano_validation.html#autotoc_md370", null ],
      [ "Latest Validation Snapshot", "contrib_orin_nano_validation.html#autotoc_md371", null ],
      [ "Prerequisites", "contrib_orin_nano_validation.html#autotoc_md373", null ],
      [ "Configure And Build", "contrib_orin_nano_validation.html#autotoc_md375", null ],
      [ "Run C++ Tests", "contrib_orin_nano_validation.html#autotoc_md377", null ],
      [ "Serial Validation", "contrib_orin_nano_validation.html#autotoc_md379", [
        [ "Automated Integration Coverage", "contrib_orin_nano_validation.html#autotoc_md380", null ],
        [ "Manual Virtual Serial Pair", "contrib_orin_nano_validation.html#autotoc_md381", null ],
        [ "Physical Loopback", "contrib_orin_nano_validation.html#autotoc_md382", null ]
      ] ],
      [ "Pass Criteria", "contrib_orin_nano_validation.html#autotoc_md384", null ],
      [ "Troubleshooting", "contrib_orin_nano_validation.html#autotoc_md386", [
        [ "Boost Not Found", "contrib_orin_nano_validation.html#autotoc_md387", null ],
        [ "Serial Tests Skip", "contrib_orin_nano_validation.html#autotoc_md388", null ],
        [ "Port-Binding Failures", "contrib_orin_nano_validation.html#autotoc_md389", null ]
      ] ],
      [ "Related Docs", "contrib_orin_nano_validation.html#autotoc_md391", null ]
    ] ],
    [ "Release Checklist", "contrib_release_checklist.html", [
      [ "1. Versioning", "contrib_release_checklist.html#autotoc_md392", null ],
      [ "2. Build And Test", "contrib_release_checklist.html#autotoc_md393", null ],
      [ "3. Packaging", "contrib_release_checklist.html#autotoc_md394", null ],
      [ "4. Documentation", "contrib_release_checklist.html#autotoc_md395", null ],
      [ "5. Release Assets", "contrib_release_checklist.html#autotoc_md396", null ],
      [ "6. Compatibility Checks", "contrib_release_checklist.html#autotoc_md397", null ],
      [ "7. Post-Release Checks", "contrib_release_checklist.html#autotoc_md398", null ]
    ] ],
    [ "User Guide", "user_index.html", [
      [ "Getting Started", "user_index.html#autotoc_md614", null ],
      [ "API Reference", "user_index.html#autotoc_md616", null ],
      [ "Generated Doxygen API Reference", "user_index.html#autotoc_md617", null ],
      [ "Tutorials", "user_index.html#autotoc_md619", null ],
      [ "Guides", "user_index.html#autotoc_md621", null ],
      [ "Unilink Quick Start Guide", "user_quickstart.html", [
        [ "Installation", "user_quickstart.html#autotoc_md664", null ],
        [ "Your First TCP Client", "user_quickstart.html#autotoc_md666", null ],
        [ "Your First TCP Server", "user_quickstart.html#autotoc_md668", null ],
        [ "Transport Tutorials", "user_quickstart.html#autotoc_md670", null ],
        [ "Common Patterns", "user_quickstart.html#autotoc_md672", [
          [ "Pattern 1: Auto-Reconnection", "user_quickstart.html#autotoc_md673", null ],
          [ "Pattern 2: Error Handling", "user_quickstart.html#autotoc_md674", null ],
          [ "Pattern 3: Connection Limits (optional)", "user_quickstart.html#autotoc_md675", null ]
        ] ],
        [ "Next Steps", "user_quickstart.html#autotoc_md677", null ],
        [ "Troubleshooting", "user_quickstart.html#autotoc_md679", [
          [ "Can't connect to server?", "user_quickstart.html#autotoc_md680", null ],
          [ "Port already in use?", "user_quickstart.html#autotoc_md681", null ],
          [ "Need independent IO thread?", "user_quickstart.html#autotoc_md682", null ]
        ] ],
        [ "Support", "user_quickstart.html#autotoc_md684", null ]
      ] ],
      [ "Installation Guide", "user_installation.html", [
        [ "Prerequisites", "user_installation.html#autotoc_md623", null ],
        [ "Installation Methods", "user_installation.html#autotoc_md624", [
          [ "Method 1: vcpkg (Recommended)", "user_installation.html#autotoc_md625", [
            [ "Step 1: Install via vcpkg", "user_installation.html#autotoc_md626", null ],
            [ "Step 2: Use in your project", "user_installation.html#autotoc_md627", null ]
          ] ],
          [ "Method 2: Install from Source", "user_installation.html#autotoc_md628", [
            [ "Option A: Repository-managed vcpkg setup", "user_installation.html#autotoc_md629", null ],
            [ "Option B: Plain CMake with existing dependencies", "user_installation.html#autotoc_md630", null ],
            [ "Option C: Plain CMake with vcpkg toolchain", "user_installation.html#autotoc_md631", null ],
            [ "Use in your project", "user_installation.html#autotoc_md632", null ]
          ] ],
          [ "Method 3: Release Packages", "user_installation.html#autotoc_md633", [
            [ "Step 1: Download and extract", "user_installation.html#autotoc_md634", null ],
            [ "Step 2: Choose an install prefix", "user_installation.html#autotoc_md635", null ],
            [ "Step 3: Use in your project", "user_installation.html#autotoc_md636", null ]
          ] ],
          [ "Method 4: Git Submodule Integration", "user_installation.html#autotoc_md637", [
            [ "Step 1: Add submodule", "user_installation.html#autotoc_md638", null ],
            [ "Step 2: Use in CMake", "user_installation.html#autotoc_md639", null ]
          ] ]
        ] ],
        [ "Packaging Notes", "user_installation.html#autotoc_md640", null ],
        [ "Build Options (Source Builds)", "user_installation.html#autotoc_md641", null ],
        [ "Next Steps", "user_installation.html#autotoc_md642", null ]
      ] ],
      [ "System Requirements", "user_requirements.html", [
        [ "System Requirements", "user_requirements.html#autotoc_md686", [
          [ "Recommended Platform", "user_requirements.html#autotoc_md687", null ],
          [ "Supported Platforms", "user_requirements.html#autotoc_md688", null ]
        ] ],
        [ "Dependencies", "user_requirements.html#autotoc_md690", [
          [ "Core Library Dependencies", "user_requirements.html#autotoc_md691", null ],
          [ "Dependency Details", "user_requirements.html#autotoc_md692", null ]
        ] ],
        [ "Compiler Requirements", "user_requirements.html#autotoc_md694", [
          [ "Minimum Compiler Versions", "user_requirements.html#autotoc_md695", null ],
          [ "C++ Standard", "user_requirements.html#autotoc_md696", null ]
        ] ],
        [ "Runtime Requirements", "user_requirements.html#autotoc_md698", [
          [ "For Applications Using unilink", "user_requirements.html#autotoc_md699", null ],
          [ "Thread Support", "user_requirements.html#autotoc_md700", null ]
        ] ],
        [ "Platform-Specific Notes", "user_requirements.html#autotoc_md702", [
          [ "Ubuntu 22.04 LTS", "user_requirements.html#autotoc_md703", null ],
          [ "Ubuntu ARM64 / Jetson Orin Nano", "user_requirements.html#autotoc_md704", null ],
          [ "Ubuntu 20.04 LTS", "user_requirements.html#autotoc_md705", null ],
          [ "Other Linux Distributions", "user_requirements.html#autotoc_md706", null ]
        ] ],
        [ "Verifying Your Environment", "user_requirements.html#autotoc_md708", [
          [ "Check Compiler Version", "user_requirements.html#autotoc_md709", null ],
          [ "Check CMake Version", "user_requirements.html#autotoc_md710", null ],
          [ "Check Boost Version", "user_requirements.html#autotoc_md711", null ],
          [ "Quick Environment Test", "user_requirements.html#autotoc_md712", null ]
        ] ],
        [ "Troubleshooting", "user_requirements.html#autotoc_md714", [
          [ "Problem: Compiler Too Old", "user_requirements.html#autotoc_md715", null ],
          [ "Problem: Boost Not Found", "user_requirements.html#autotoc_md716", null ],
          [ "Problem: CMake Too Old", "user_requirements.html#autotoc_md717", null ]
        ] ],
        [ "Next Steps", "user_requirements.html#autotoc_md719", null ]
      ] ],
      [ "Unilink API Guide", "user_api_guide.html", [
        [ "Table of Contents", "user_api_guide.html#autotoc_md490", null ],
        [ "Builder API", "user_api_guide.html#autotoc_md492", [
          [ "Core Concept", "user_api_guide.html#autotoc_md493", null ],
          [ "Common Methods (All Builders)", "user_api_guide.html#autotoc_md494", null ],
          [ "Callback Registration Policy", "user_api_guide.html#autotoc_md495", null ],
          [ "<tt>MessageContext</tt> Data Ownership", "user_api_guide.html#autotoc_md496", null ],
          [ "Framed Message Handling", "user_api_guide.html#autotoc_md497", null ],
          [ "Move And Shared Buffer Sends", "user_api_guide.html#autotoc_md498", null ],
          [ "Socket Tuning Options", "user_api_guide.html#autotoc_md499", null ],
          [ "IO Context Ownership (advanced)", "user_api_guide.html#autotoc_md500", null ],
          [ "Starting Synchronously vs. Asynchronously", "user_api_guide.html#autotoc_md501", [
            [ "Asynchronous Example", "user_api_guide.html#autotoc_md502", null ]
          ] ]
        ] ],
        [ "TCP Client", "user_api_guide.html#autotoc_md504", [
          [ "Basic Usage", "user_api_guide.html#autotoc_md505", null ],
          [ "API Reference", "user_api_guide.html#autotoc_md506", [
            [ "Constructor", "user_api_guide.html#autotoc_md507", null ],
            [ "Builder Methods", "user_api_guide.html#autotoc_md508", null ],
            [ "Instance Methods", "user_api_guide.html#autotoc_md509", null ]
          ] ],
          [ "Advanced Examples", "user_api_guide.html#autotoc_md510", [
            [ "With Member Functions", "user_api_guide.html#autotoc_md511", null ],
            [ "With Lambda Capture", "user_api_guide.html#autotoc_md512", null ]
          ] ]
        ] ],
        [ "TCP Server", "user_api_guide.html#autotoc_md514", [
          [ "Basic Usage", "user_api_guide.html#autotoc_md515", null ],
          [ "API Reference", "user_api_guide.html#autotoc_md516", [
            [ "Constructor", "user_api_guide.html#autotoc_md517", null ],
            [ "Builder Methods", "user_api_guide.html#autotoc_md518", null ],
            [ "Instance Methods", "user_api_guide.html#autotoc_md519", null ]
          ] ],
          [ "Advanced Examples", "user_api_guide.html#autotoc_md520", [
            [ "Single Client Mode", "user_api_guide.html#autotoc_md521", null ],
            [ "Port Retry", "user_api_guide.html#autotoc_md522", null ],
            [ "Echo Server Pattern", "user_api_guide.html#autotoc_md523", null ]
          ] ]
        ] ],
        [ "Serial Communication", "user_api_guide.html#autotoc_md525", [
          [ "Basic Usage", "user_api_guide.html#autotoc_md526", null ],
          [ "API Reference", "user_api_guide.html#autotoc_md527", [
            [ "Constructor", "user_api_guide.html#autotoc_md528", null ],
            [ "Builder Methods", "user_api_guide.html#autotoc_md529", null ],
            [ "Instance Methods", "user_api_guide.html#autotoc_md530", null ]
          ] ],
          [ "Device Paths", "user_api_guide.html#autotoc_md531", null ],
          [ "Advanced Examples", "user_api_guide.html#autotoc_md532", [
            [ "Arduino Communication", "user_api_guide.html#autotoc_md533", null ],
            [ "GPS Module", "user_api_guide.html#autotoc_md534", null ]
          ] ]
        ] ],
        [ "UDP Communication", "user_api_guide.html#autotoc_md536", [
          [ "Basic Usage", "user_api_guide.html#autotoc_md537", [
            [ "UDP Receiver (Server)", "user_api_guide.html#autotoc_md538", null ],
            [ "UDP Sender (Client)", "user_api_guide.html#autotoc_md539", null ]
          ] ],
          [ "API Reference", "user_api_guide.html#autotoc_md540", [
            [ "Constructors", "user_api_guide.html#autotoc_md541", null ],
            [ "Builder Methods (UdpClient)", "user_api_guide.html#autotoc_md542", null ],
            [ "Builder Methods (UdpServer)", "user_api_guide.html#autotoc_md543", null ],
            [ "Instance Methods (UdpClient)", "user_api_guide.html#autotoc_md544", null ]
          ] ],
          [ "Advanced Examples", "user_api_guide.html#autotoc_md545", [
            [ "Echo Reply (Receiver)", "user_api_guide.html#autotoc_md546", null ],
            [ "UDP Server (Receive-only listener)", "user_api_guide.html#autotoc_md547", null ]
          ] ]
        ] ],
        [ "UDS Communication", "user_api_guide.html#autotoc_md549", [
          [ "Basic Usage", "user_api_guide.html#autotoc_md550", [
            [ "UDS Server", "user_api_guide.html#autotoc_md551", null ],
            [ "UDS Client", "user_api_guide.html#autotoc_md552", null ]
          ] ],
          [ "API Reference", "user_api_guide.html#autotoc_md553", [
            [ "Constructors", "user_api_guide.html#autotoc_md554", null ],
            [ "Builder Methods (UDS Server)", "user_api_guide.html#autotoc_md555", null ],
            [ "Builder Methods (UDS Client)", "user_api_guide.html#autotoc_md556", null ],
            [ "Instance Methods (UDS Client)", "user_api_guide.html#autotoc_md557", null ],
            [ "Instance Methods (UDS Server)", "user_api_guide.html#autotoc_md558", null ]
          ] ],
          [ "Notes on UDS", "user_api_guide.html#autotoc_md559", null ]
        ] ],
        [ "Error Handling", "user_api_guide.html#autotoc_md561", [
          [ "Setup Error Handler", "user_api_guide.html#autotoc_md562", null ],
          [ "Error Levels", "user_api_guide.html#autotoc_md563", null ],
          [ "Error Statistics", "user_api_guide.html#autotoc_md564", null ]
        ] ],
        [ "Logging System", "user_api_guide.html#autotoc_md566", [
          [ "Basic Usage", "user_api_guide.html#autotoc_md567", null ],
          [ "Log Levels", "user_api_guide.html#autotoc_md568", null ],
          [ "Async Logging", "user_api_guide.html#autotoc_md569", null ],
          [ "Custom Format", "user_api_guide.html#autotoc_md570", null ],
          [ "Environment", "user_api_guide.html#autotoc_md571", null ]
        ] ],
        [ "Configuration Management", "user_api_guide.html#autotoc_md573", [
          [ "Load Configuration from File", "user_api_guide.html#autotoc_md574", null ],
          [ "Configuration File Format", "user_api_guide.html#autotoc_md575", null ]
        ] ],
        [ "Best Practices", "user_api_guide.html#autotoc_md577", [
          [ "1. Always Handle Errors", "user_api_guide.html#autotoc_md578", null ],
          [ "2. Use Explicit Lifecycle Control", "user_api_guide.html#autotoc_md579", null ],
          [ "3. Set Appropriate Retry Intervals", "user_api_guide.html#autotoc_md580", null ],
          [ "4. Enable Logging for Debugging", "user_api_guide.html#autotoc_md581", null ],
          [ "5. Use Member Functions for OOP Design", "user_api_guide.html#autotoc_md582", null ]
        ] ],
        [ "Performance Tips", "user_api_guide.html#autotoc_md584", [
          [ "1. Use Independent Context for Testing Only", "user_api_guide.html#autotoc_md585", null ],
          [ "2. Enable Async Logging", "user_api_guide.html#autotoc_md586", null ]
        ] ],
        [ "Backpressure Strategy", "user_api_guide.html#autotoc_md588", [
          [ "Strategies", "user_api_guide.html#autotoc_md589", null ],
          [ "Send And Backpressure Semantics", "user_api_guide.html#autotoc_md590", [
            [ "Reliable", "user_api_guide.html#autotoc_md591", null ],
            [ "BestEffort", "user_api_guide.html#autotoc_md592", null ],
            [ "Throughput Interpretation", "user_api_guide.html#autotoc_md593", null ]
          ] ],
          [ "Runtime Statistics", "user_api_guide.html#autotoc_md594", null ],
          [ "When to use each", "user_api_guide.html#autotoc_md595", null ],
          [ "C++ Usage", "user_api_guide.html#autotoc_md596", null ],
          [ "Thresholds", "user_api_guide.html#autotoc_md597", null ],
          [ "Transport Meaning", "user_api_guide.html#autotoc_md598", null ]
        ] ],
        [ "Security", "user_api_guide.html#autotoc_md600", [
          [ "Validate All Input", "user_api_guide.html#autotoc_md601", null ],
          [ "Rate Limiting", "user_api_guide.html#autotoc_md602", null ],
          [ "Connection Limits", "user_api_guide.html#autotoc_md603", null ]
        ] ]
      ] ],
      [ "Tutorials", "user_tutorials.html", [
        [ "Getting Started with Unilink", "tutorial_01.html", [
          [ "What You'll Build", "tutorial_01.html#autotoc_md814", null ],
          [ "Step 1: Create The Client", "tutorial_01.html#autotoc_md816", null ],
          [ "Step 2: Build With CMake", "tutorial_01.html#autotoc_md818", null ],
          [ "Step 3: Run Against A Test Server", "tutorial_01.html#autotoc_md820", null ],
          [ "API Patterns Used In This Tutorial", "tutorial_01.html#autotoc_md822", null ],
          [ "Use The Full Example If You Want More", "tutorial_01.html#autotoc_md824", null ],
          [ "Next Steps", "tutorial_01.html#autotoc_md826", null ]
        ] ],
        [ "Building a TCP Server", "tutorial_02.html", [
          [ "What You'll Build", "tutorial_02.html#autotoc_md829", null ],
          [ "Step 1: Create The Server", "tutorial_02.html#autotoc_md831", null ],
          [ "Step 2: Run It", "tutorial_02.html#autotoc_md833", null ],
          [ "Step 3: Understand The Current Server API", "tutorial_02.html#autotoc_md835", null ],
          [ "Client Limits", "tutorial_02.html#autotoc_md837", null ],
          [ "Use The Full Example Programs For More", "tutorial_02.html#autotoc_md839", null ],
          [ "Next Steps", "tutorial_02.html#autotoc_md841", null ]
        ] ],
        [ "UDS Communication", "tutorial_03.html", [
          [ "What You'll Build", "tutorial_03.html#autotoc_md844", null ],
          [ "Step 1: Create A UDS Server", "tutorial_03.html#autotoc_md846", null ],
          [ "Step 2: Create A UDS Client", "tutorial_03.html#autotoc_md848", null ],
          [ "Why Use UDS Instead Of TCP", "tutorial_03.html#autotoc_md850", null ],
          [ "Operational Notes", "tutorial_03.html#autotoc_md852", null ],
          [ "Next Steps", "tutorial_03.html#autotoc_md854", null ]
        ] ],
        [ "Serial Communication", "tutorial_04.html", [
          [ "What You'll Build", "tutorial_04.html#autotoc_md857", null ],
          [ "Step 1: Choose A Device Path", "tutorial_04.html#autotoc_md859", null ],
          [ "Step 2: Create A Minimal Serial Terminal", "tutorial_04.html#autotoc_md861", null ],
          [ "Step 3: Build And Run", "tutorial_04.html#autotoc_md863", null ],
          [ "Step 4: Test With A Second Terminal", "tutorial_04.html#autotoc_md865", null ],
          [ "Common Adjustments", "tutorial_04.html#autotoc_md867", null ],
          [ "When To Use The Example Programs Instead", "tutorial_04.html#autotoc_md869", null ],
          [ "Next Steps", "tutorial_04.html#autotoc_md871", null ]
        ] ],
        [ "UDP Communication", "tutorial_05.html", [
          [ "What You'll Build", "tutorial_05.html#autotoc_md874", null ],
          [ "Step 1: Create A Receiver", "tutorial_05.html#autotoc_md876", null ],
          [ "Step 2: Create A Sender", "tutorial_05.html#autotoc_md878", null ],
          [ "Step 3: Build The Two Programs", "tutorial_05.html#autotoc_md880", null ],
          [ "Step 4: Run Both Programs", "tutorial_05.html#autotoc_md882", null ],
          [ "What Is Different About UDP", "tutorial_05.html#autotoc_md884", null ],
          [ "Practical Notes", "tutorial_05.html#autotoc_md886", null ],
          [ "Use The Full Examples For Repeated Testing", "tutorial_05.html#autotoc_md888", null ],
          [ "Next Steps", "tutorial_05.html#autotoc_md890", null ]
        ] ]
      ] ],
      [ "Troubleshooting Guide", "user_troubleshooting.html", [
        [ "Table of Contents", "user_troubleshooting.html#autotoc_md733", null ],
        [ "Connection Issues", "user_troubleshooting.html#autotoc_md735", [
          [ "Problem: Connection Refused", "user_troubleshooting.html#autotoc_md736", [
            [ "1. Server Not Running", "user_troubleshooting.html#autotoc_md737", null ],
            [ "2. Wrong Host/Port", "user_troubleshooting.html#autotoc_md738", null ],
            [ "3. Firewall Blocking", "user_troubleshooting.html#autotoc_md739", null ]
          ] ],
          [ "Problem: Connection Timeout", "user_troubleshooting.html#autotoc_md741", [
            [ "1. Network Unreachable", "user_troubleshooting.html#autotoc_md742", null ],
            [ "2. Server Overloaded", "user_troubleshooting.html#autotoc_md743", null ]
          ] ],
          [ "Problem: Connection Drops Randomly", "user_troubleshooting.html#autotoc_md745", [
            [ "1. Network Instability", "user_troubleshooting.html#autotoc_md746", null ],
            [ "2. Server Closing Connection", "user_troubleshooting.html#autotoc_md747", null ],
            [ "3. Keep-Alive Not Set", "user_troubleshooting.html#autotoc_md748", null ]
          ] ],
          [ "Problem: Port Already in Use", "user_troubleshooting.html#autotoc_md750", [
            [ "1. Kill Existing Process", "user_troubleshooting.html#autotoc_md751", null ],
            [ "2. Use Different Port", "user_troubleshooting.html#autotoc_md752", null ],
            [ "3. Enable Port Retry", "user_troubleshooting.html#autotoc_md753", null ]
          ] ]
        ] ],
        [ "Compilation Errors", "user_troubleshooting.html#autotoc_md755", [
          [ "Problem: unilink/unilink.hpp Not Found", "user_troubleshooting.html#autotoc_md756", [
            [ "1. Install unilink", "user_troubleshooting.html#autotoc_md757", null ],
            [ "2. Add Include Path", "user_troubleshooting.html#autotoc_md758", null ],
            [ "3. Use as Subdirectory", "user_troubleshooting.html#autotoc_md759", null ]
          ] ],
          [ "Problem: Undefined Reference to unilink Symbols", "user_troubleshooting.html#autotoc_md761", [
            [ "1. Link unilink Library", "user_troubleshooting.html#autotoc_md762", null ],
            [ "2. Check Library Path", "user_troubleshooting.html#autotoc_md763", null ]
          ] ],
          [ "Problem: Boost Not Found", "user_troubleshooting.html#autotoc_md765", [
            [ "Source-build vcpkg setup", "user_troubleshooting.html#autotoc_md766", null ],
            [ "System Boost setup", "user_troubleshooting.html#autotoc_md767", null ],
            [ "Windows source builds with vcpkg", "user_troubleshooting.html#autotoc_md768", null ],
            [ "Manual Boost Path", "user_troubleshooting.html#autotoc_md769", null ]
          ] ]
        ] ],
        [ "Runtime Errors", "user_troubleshooting.html#autotoc_md771", [
          [ "Problem: Segmentation Fault", "user_troubleshooting.html#autotoc_md772", [
            [ "1. Enable Core Dumps", "user_troubleshooting.html#autotoc_md773", null ],
            [ "2. Common Causes", "user_troubleshooting.html#autotoc_md774", null ]
          ] ],
          [ "Problem: Callbacks Not Being Called", "user_troubleshooting.html#autotoc_md776", [
            [ "1. Receive Callback Not Registered", "user_troubleshooting.html#autotoc_md777", null ],
            [ "2. Client Not Started", "user_troubleshooting.html#autotoc_md778", null ],
            [ "3. Application Exits Too Quickly", "user_troubleshooting.html#autotoc_md779", null ]
          ] ],
          [ "Problem: UDP with Reliable Strategy Still Drops Packets", "user_troubleshooting.html#autotoc_md781", null ]
        ] ],
        [ "Performance Issues", "user_troubleshooting.html#autotoc_md783", [
          [ "Problem: High CPU Usage", "user_troubleshooting.html#autotoc_md784", [
            [ "1. Busy Loop in Callback", "user_troubleshooting.html#autotoc_md785", null ],
            [ "2. Too Many Retries", "user_troubleshooting.html#autotoc_md786", null ],
            [ "3. Excessive Logging", "user_troubleshooting.html#autotoc_md787", null ]
          ] ],
          [ "Problem: High Memory Usage", "user_troubleshooting.html#autotoc_md789", [
            [ "1. Fix Memory Leaks", "user_troubleshooting.html#autotoc_md790", null ],
            [ "3. Limit Buffer Sizes", "user_troubleshooting.html#autotoc_md791", null ]
          ] ],
          [ "Problem: Slow Data Transfer", "user_troubleshooting.html#autotoc_md793", [
            [ "1. Batch Small Messages", "user_troubleshooting.html#autotoc_md794", null ],
            [ "2. Use Binary Protocol", "user_troubleshooting.html#autotoc_md795", null ],
            [ "3. Enable Async Logging", "user_troubleshooting.html#autotoc_md796", null ]
          ] ]
        ] ],
        [ "Memory Issues", "user_troubleshooting.html#autotoc_md798", [
          [ "Problem: Memory Leak Detected", "user_troubleshooting.html#autotoc_md799", null ]
        ] ],
        [ "Thread Safety Issues", "user_troubleshooting.html#autotoc_md801", [
          [ "Problem: Race Condition / Data Corruption", "user_troubleshooting.html#autotoc_md802", [
            [ "1. Protect Shared State", "user_troubleshooting.html#autotoc_md803", null ]
          ] ]
        ] ],
        [ "Debugging Tips", "user_troubleshooting.html#autotoc_md805", [
          [ "Enable Debug Logging", "user_troubleshooting.html#autotoc_md806", null ],
          [ "Use GDB for Debugging", "user_troubleshooting.html#autotoc_md807", null ],
          [ "Network Debugging with tcpdump", "user_troubleshooting.html#autotoc_md808", null ],
          [ "Test with netcat", "user_troubleshooting.html#autotoc_md809", null ]
        ] ],
        [ "Getting Help", "user_troubleshooting.html#autotoc_md811", null ]
      ] ],
      [ "Python Bindings", "user_python_bindings.html", null ],
      [ "Performance Guide", "user_performance.html", [
        [ "Table of Contents", "user_performance.html#autotoc_md644", null ],
        [ "Runtime Optimization", "user_performance.html#autotoc_md646", [
          [ "1. Threading Model & IO Context", "user_performance.html#autotoc_md647", null ],
          [ "2. Async Logging", "user_performance.html#autotoc_md648", null ],
          [ "3. Non-Blocking Callbacks", "user_performance.html#autotoc_md649", null ]
        ] ],
        [ "Memory Optimization", "user_performance.html#autotoc_md651", [
          [ "1. Avoid Data Copies", "user_performance.html#autotoc_md652", null ],
          [ "2. Reserve Vector Capacity", "user_performance.html#autotoc_md653", null ]
        ] ],
        [ "Network Optimization", "user_performance.html#autotoc_md655", [
          [ "1. Batch Small Messages", "user_performance.html#autotoc_md656", null ],
          [ "2. Connection Reuse", "user_performance.html#autotoc_md657", null ],
          [ "3. Socket Tuning", "user_performance.html#autotoc_md658", null ]
        ] ],
        [ "Backpressure Management", "user_performance.html#backpressure-management", [
          [ "1. Choosing a Strategy", "user_performance.html#autotoc_md660", null ],
          [ "Interpreting Strategy Results", "user_performance.html#autotoc_md661", null ],
          [ "2. High-Throughput Sensors (LiDAR/Camera)", "user_performance.html#autotoc_md662", null ],
          [ "3. Critical Reliable Data", "user_performance.html#autotoc_md663", null ]
        ] ]
      ] ]
    ] ],
    [ "API Stability Policy", "user_api_stability.html", [
      [ "Stable Public Surface", "user_api_stability.html#autotoc_md604", null ],
      [ "Supported But Advanced Headers", "user_api_stability.html#autotoc_md605", null ],
      [ "Internal Or Not Source-Stable Before v1.0", "user_api_stability.html#autotoc_md606", null ],
      [ "Source Compatibility", "user_api_stability.html#autotoc_md607", null ],
      [ "ABI Compatibility", "user_api_stability.html#autotoc_md608", null ],
      [ "Design-Only APIs", "user_api_stability.html#autotoc_md609", null ],
      [ "Deprecation Policy", "user_api_stability.html#autotoc_md610", null ],
      [ "Python Bindings", "user_api_stability.html#autotoc_md611", null ],
      [ "Recommended Include Policy", "user_api_stability.html#autotoc_md612", null ]
    ] ],
    [ "Transport Feature Matrix", "user_transport_matrix.html", [
      [ "Overview", "user_transport_matrix.html#autotoc_md720", null ],
      [ "Transport Families", "user_transport_matrix.html#autotoc_md721", null ],
      [ "Lifecycle Support", "user_transport_matrix.html#autotoc_md722", null ],
      [ "Send API Support", "user_transport_matrix.html#autotoc_md723", null ],
      [ "Server API Support", "user_transport_matrix.html#autotoc_md724", null ],
      [ "Backpressure Support", "user_transport_matrix.html#autotoc_md725", null ],
      [ "Runtime Statistics Support", "user_transport_matrix.html#autotoc_md726", null ],
      [ "Framing Support", "user_transport_matrix.html#autotoc_md727", null ],
      [ "Reconnect / Retry Support", "user_transport_matrix.html#autotoc_md728", null ],
      [ "Socket Tuning Support", "user_transport_matrix.html#autotoc_md729", null ],
      [ "Platform Notes", "user_transport_matrix.html#autotoc_md730", null ],
      [ "Planned / Design-Only APIs", "user_transport_matrix.html#autotoc_md731", null ]
    ] ],
    [ "Asynchronous Programming Patterns", "user_tutorial_async.html", [
      [ "1. Non-Blocking Startup", "user_tutorial_async.html#autotoc_md893", [
        [ "The Async Pattern", "user_tutorial_async.html#autotoc_md894", null ]
      ] ],
      [ "2. Shared Ownership in Callbacks", "user_tutorial_async.html#autotoc_md896", [
        [ "Safe Capture Pattern", "user_tutorial_async.html#autotoc_md897", null ]
      ] ],
      [ "3. Parallel Initialization", "user_tutorial_async.html#autotoc_md899", null ],
      [ "4. When to Use Async vs Sync", "user_tutorial_async.html#autotoc_md901", null ],
      [ "Summary", "user_tutorial_async.html#autotoc_md903", null ]
    ] ],
    [ "unilink-docs", "md__2home_2runner_2work_2unilink-docs_2unilink-docs_2README.html", [
      [ "Documentation", "md__2home_2runner_2work_2unilink-docs_2unilink-docs_2README.html#autotoc_md905", null ],
      [ "Local validation", "md__2home_2runner_2work_2unilink-docs_2unilink-docs_2README.html#autotoc_md906", null ]
    ] ],
    [ "Namespaces", "namespaces.html", [
      [ "Namespace List", "namespaces.html", null ],
      [ "Namespace Members", "namespacemembers.html", [
        [ "All", "namespacemembers.html", null ],
        [ "Functions", "namespacemembers_func.html", null ]
      ] ]
    ] ],
    [ "Concepts", "concepts.html", "concepts" ],
    [ "Classes", "annotated.html", [
      [ "Class List", "annotated.html", "annotated_dup" ],
      [ "Class Index", "classes.html", null ],
      [ "Class Hierarchy", "hierarchy.html", "hierarchy" ],
      [ "Class Members", "functions.html", [
        [ "All", "functions.html", "functions_dup" ],
        [ "Functions", "functions_func.html", "functions_func" ],
        [ "Typedefs", "functions_type.html", null ]
      ] ]
    ] ],
    [ "Files", "files.html", [
      [ "File List", "files.html", "files_dup" ]
    ] ]
  ] ]
];

var NAVTREEINDEX =
[
"annotated.html",
"classunilink_1_1wrapper_1_1ServerInterface.html#a637fa1fc50c1777ef53e69204da6f72d",
"contrib_arch_runtime.html#autotoc_md173",
"dir_dd7c4a114ea21f716c1c2a74faf9fd37.html",
"user_api_guide.html#autotoc_md512",
"user_troubleshooting.html#autotoc_md805"
];

var SYNCONMSG = 'click to disable panel synchronisation';
var SYNCOFFMSG = 'click to enable panel synchronisation';
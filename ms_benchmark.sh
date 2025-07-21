#!/bin/bash

# Out-of-core MergeSort Performance Benchmark Suite
# Iterates through parameter combinations and generates performance graphs

set -e  # Exit on any error

# =============================================================================
# CONFIGURATION SECTION
# =============================================================================

PROGRAM_EXECUTABLE="./mergesort"

# SLURM job parameters
SLURM_MEM="8G" # Not used
SLURM_TIME="00:30:00" #Not used
SLURM_PARTITION="normal"

# Output directories
RESULTS_DIR="benchmark_results"
PLOTS_DIR="performance_plots"
LOGS_DIR="benchmark_logs"

R_SCRIPT="ms_plot_script.r"

# Parameters to iterate over
EXECUTION_POLICIES=("OMP" "FastFlow" "MPI_FF")
NUM_PROCESSES=(4 6 7)
NUM_THREADS=(4 8 16)
CHUNK_SIZES=(500000 1000000 5000000 10000000)
RECORD_COUNTS=(1000000 4000000 10000000 50000000) # 500MB 2GB 5GB 25GB
NUM_RUNS=3  # Number of repetitions for averaging

# Timing and progress
START_TIME=$(date +%s)
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

# Print colored output
print_header() {
    echo -e "\n\033[1;34m=== $1 ===\033[0m"
}

print_info() {
    echo -e "\033[1;32m[INFO]\033[0m $1"
}

print_warning() {
    echo -e "\033[1;33m[WARNING]\033[0m $1"
}

print_error() {
    echo -e "\033[1;31m[ERROR]\033[0m $1"
}

# Create necessary directories
setup_directories() {
    print_info "Setting up directories..."
    mkdir -p "$RESULTS_DIR" "$PLOTS_DIR" "$LOGS_DIR"
    
    # Create timestamped subdirectories
    RESULTS_DIR="${RESULTS_DIR}/run_${TIMESTAMP}"
    PLOTS_DIR="${PLOTS_DIR}/run_${TIMESTAMP}"
    LOGS_DIR="${LOGS_DIR}/run_${TIMESTAMP}"
    
    mkdir -p "$RESULTS_DIR" "$PLOTS_DIR" "$LOGS_DIR"
    
    print_info "Results will be saved to: $RESULTS_DIR"
    print_info "Plots will be saved to: $PLOTS_DIR"
    print_info "Logs will be saved to: $LOGS_DIR"
}

# Calculate total number of jobs
calculate_total_jobs() {
    local total=0
    for policy in "${EXECUTION_POLICIES[@]}"; do
        if [[ "$policy" == "MPI_FF" ]]; then
            # MPI_FF uses both processes and threads
            total=$((total + ${#NUM_PROCESSES[@]} * ${#NUM_THREADS[@]} * ${#CHUNK_SIZES[@]} * ${#RECORD_COUNTS[@]} * NUM_RUNS))
        else
            # OMP and FastFlow only use threads (processes = 1)
            total=$((total + 1 * ${#NUM_THREADS[@]} * ${#CHUNK_SIZES[@]} * ${#RECORD_COUNTS[@]} * NUM_RUNS))
        fi
    done
    echo $total
}

# Validate prerequisites
validate_setup() {
    print_header "Validating Setup"
    
    # Check if executable exists
    if [[ ! -x "$PROGRAM_EXECUTABLE" ]]; then
        print_error "Program executable not found or not executable: $PROGRAM_EXECUTABLE"
        print_info "Please compile your program and ensure the executable path is correct."
        exit 1
    fi
    
    # Check if R script exists
    if [[ ! -f "$R_SCRIPT" ]]; then
        print_error "R script not found: $R_SCRIPT"
        exit 1
    fi
    
    # Check if R is available
    if ! command -v Rscript &> /dev/null; then
        print_error "Rscript not found. Will run without generating plots"
        R_PLOT=false
    else
        R_PLOT=true
        print_info "Rscript detected. Will generate plots."
    fi

    # Check if srun is available (for SLURM clusters)
    if ! command -v srun &> /dev/null; then
        print_warning "srun not found. Will run jobs directly without SLURM."
        USE_SLURM=false
    else
        USE_SLURM=true
        print_info "SLURM detected. Will use srun for job execution."
    fi
    
    print_info "Setup validation complete."
}

# Run a single benchmark job
run_benchmark_job() {
    local policy=$1
    local processes=$2
    local threads=$3
    local chunk_size=$4
    local record_count=$5
    local run_number=$6
    local job_id=$7
    local total_jobs=$8
    
    local job_name="${policy}_p${processes}_t${threads}_c${chunk_size}_r${record_count}_run${run_number}"
    local log_file="${LOGS_DIR}/${job_name}.log"
    local csv_file="${RESULTS_DIR}/ms_test_${TIMESTAMP}.csv"
    
    print_info "[$job_id/$total_jobs] Running: $job_name"
    
    # Prepare command
    local cmd="$PROGRAM_EXECUTABLE $policy $processes $threads $chunk_size $record_count $csv_file"
    
    # Execute with SLURM, mpirun or normal local execution
    if [[ "$USE_SLURM" == true ]]; then
        # Use SLURM srun
        srun --time="$SLURM_TIME" \
             --nodes="$processes" \
             --ntasks="$processes" \
             --ntasks-per-node="1" \
             --cpus-per-task="$threads" \
             --cpu-bind="none" \
             --mpi="pmix" \
             --output="$log_file" \
             --error="${log_file}.err" \
             $cmd
             #--job-name="$job_name" \
    elif [[ "$policy" == "MPI_FF" ]]; then
        echo "Executing: mpirun -n $processes $cmd" > "$log_file"
        mpirun -n $processes $cmd >> "$log_file" 2>&1
    else
        # Run directly
        echo "Executing: $cmd" > "$log_file"
        $cmd >> "$log_file" 2>&1
    fi
    
    local exit_code=$?
    if [[ $exit_code -eq 0 ]]; then
        print_info "[$job_id/$total_jobs] Completed: $job_name"
    else
        print_error "[$job_id/$total_jobs] Failed: $job_name (exit code: $exit_code)"
        print_error "Check log file: $log_file"
        return $exit_code
    fi
}

# Run all benchmark combinations
run_all_benchmarks() {
    print_header "Running Benchmark Suite"
    
    local total_jobs=$(calculate_total_jobs)
    local current_job=0
    local failed_jobs=0
    
    print_info "Total jobs to execute: $total_jobs"
    
    # Create CSV header
    #local main_csv="${RESULTS_DIR}/ms_test_${TIMESTAMP}.csv"
    # echo "execution_policy,num_processes,num_threads,chunk_size,record_count,completion_time_ms,run_number,timestamp" > "$main_csv"
    
    # Iterate through all parameter combinations
    for record_count in "${RECORD_COUNTS[@]}"; do
        ./record_gen "$record_count"
        for policy in "${EXECUTION_POLICIES[@]}"; do
            print_info "Processing execution policy: $policy"
            if [[ "$policy" == "MPI_FF" ]]; then
                # MPI_FF: iterate over both processes and threads
                    for processes in "${NUM_PROCESSES[@]}"; do
                        for threads in "${NUM_THREADS[@]}"; do
                            for chunk_size in "${CHUNK_SIZES[@]}"; do
                                for run in $(seq 1 $NUM_RUNS); do
                                    current_job=$((current_job + 1))
                                    
                                    if ! run_benchmark_job "$policy" "$processes" "$threads" "$chunk_size" "$record_count" "$run" "$current_job" "$total_jobs"; then
                                        failed_jobs=$((failed_jobs + 1))
                                    fi
                                    
                                    sleep 1
                                done
                            done
                        done
                    done
            else
                # OMP and FastFlow: only iterate over threads (processes = 1)
                local processes=1
                for threads in "${NUM_THREADS[@]}"; do
                    for chunk_size in "${CHUNK_SIZES[@]}"; do
                        for run in $(seq 1 $NUM_RUNS); do
                            current_job=$((current_job + 1))
                            
                            if ! run_benchmark_job "$policy" "$processes" "$threads" "$chunk_size" "$record_count" "$run" "$current_job" "$total_jobs"; then
                                failed_jobs=$((failed_jobs + 1))
                            fi
                            
                            sleep 1
                        done
                    done
                done
            fi
        done
    done
    
    print_info "Benchmark execution completed!"
    print_info "Total jobs: $total_jobs, Failed: $failed_jobs, Success: $((total_jobs - failed_jobs))"
    
    return $failed_jobs
}

# Generate performance graphs using R
generate_graphs() {
    print_header "Generating Performance Graphs"
    
    local main_csv="${RESULTS_DIR}/ms_test_${TIMESTAMP}.csv"
    
    if [[ ! -f "$main_csv" ]]; then
        print_error "Results CSV file not found: $main_csv"
        return 1
    fi
    
    local line_count=$(wc -l < "$main_csv")
    if [[ $line_count -le 1 ]]; then
        print_error "Results CSV file appears to be empty (only header): $main_csv"
        return 1
    fi
    
    print_info "Processing results from: $main_csv"
    print_info "Total data points: $((line_count - 1))"
    
    # Run R script
    local param_description="Benchmark Run ${TIMESTAMP}"
    print_info "Executing R analysis script..."
    
    if [[ "$R_PLOT" == true ]]; then
        if Rscript "$R_SCRIPT" "$main_csv" "$TIMESTAMP" "$PLOTS_DIR"; then
            print_info "Graphs generated successfully in: $PLOTS_DIR"
        else
            print_error "Failed to generate graphs"
            return 1
        fi
    fi
}

# Generate summary report
generate_summary_report() {
    print_header "Generating Summary Report"
    
    local report_file="${RESULTS_DIR}/benchmark_summary_${TIMESTAMP}.txt"
    local main_csv="${RESULTS_DIR}/ms_test_${TIMESTAMP}.csv"
    
    {
        echo "========================================"
        echo "        BENCHMARK SUMMARY REPORT        "
        echo "========================================"
        echo "Generated: $(date)"
        echo "Benchmark Run ID: $TIMESTAMP"
        echo ""
        echo "CONFIGURATION:"
        echo "- Execution Policies: ${EXECUTION_POLICIES[*]}"
        echo "- Number of Processes: ${NUM_PROCESSES[*]}"
        echo "- Number of Threads: ${NUM_THREADS[*]}"
        echo "- Chunk Sizes: ${CHUNK_SIZES[*]}"
        echo "- Record Counts: ${RECORD_COUNTS[*]}"
        echo "- Number of Runs: $NUM_RUNS"
        echo ""
        echo "RESULTS:"
        if [[ -f "$main_csv" ]]; then
            local total_measurements=$(tail -n +2 "$main_csv" | wc -l)
            echo "- Total Measurements: $total_measurements"
            echo "- Results File: $main_csv"
            echo "- Plots Directory: $PLOTS_DIR"
            echo ""
            echo "DATA PREVIEW (first 10 lines):"
            head -n 11 "$main_csv"
        else
            echo "- No results file found"
        fi
        echo ""
        echo "EXECUTION TIME:"
        local end_time=$(date +%s)
        local duration=$((end_time - START_TIME))
        local hours=$((duration / 3600))
        local minutes=$(((duration % 3600) / 60))
        local seconds=$((duration % 60))
        echo "- Total Duration: ${hours}h ${minutes}m ${seconds}s"
        echo ""
        echo "========================================"
    } > "$report_file"
    
    print_info "Summary report saved to: $report_file"
    
    # Also print to console
    cat "$report_file"
}

# Clean up function
cleanup() {
    print_info "Cleaning up temporary files..."
    # Add any cleanup tasks here if needed
}

# Signal handlers
trap cleanup EXIT
trap 'print_error "Script interrupted"; exit 1' INT TERM

# =============================================================================
# MAIN EXECUTION
# =============================================================================

main() {
    print_header "MergeSort Performance Benchmark Suite"
    print_info "Starting benchmark run at $(date)"
    print_info "Run ID: $TIMESTAMP"
    
    # Setup and validation
    validate_setup
    setup_directories
    
    # Run benchmarks
    if run_all_benchmarks; then
        print_info "All benchmarks completed successfully"
    else
        print_warning "Some benchmarks failed. Check logs in: $LOGS_DIR"
    fi
    
    # Generate graphs
    #generate_graphs
    
    # Generate summary
    generate_summary_report
    
    print_header "Benchmark Suite Complete"
    print_info "Run ID: $TIMESTAMP"
    print_info "Results: $RESULTS_DIR"
    print_info "Plots: $PLOTS_DIR"
    print_info "Logs: $LOGS_DIR"
}

# =============================================================================
# SCRIPT ENTRY POINT
# =============================================================================

# Parse command line options
while [[ $# -gt 0 ]]; do
    case $1 in
        --executable)
            PROGRAM_EXECUTABLE="$2"
            shift 2
            ;;
        #--partition)
        #    SLURM_PARTITION="$2"
        #    shift 2
        #    ;;
        #--mem)
        #    SLURM_MEM="$2"
        #    shift 2
        #    ;;
        --time)
            SLURM_TIME="$2"
            shift 2
            ;;
        
        --runs)
            NUM_RUNS="$2"
            shift 2
            ;;
        --help|-h)
            echo "MergeSort Performance Benchmark Suite"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --executable PATH    Path to benchmark executable (default: ./mergesort)"
            echo "  --time TIME         SLURM time limit (default: 00:30:00)"
            #echo "  --partition NAME     SLURM partition name (default: compute)"
            #echo "  --mem MEMORY        SLURM memory limit (default: 8G)"
            echo "  --runs NUMBER       Number of runs per configuration (default: 2)"
            echo "  --help, -h          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                                    # Run with defaults"
            echo "  $0 --executable ./my_benchmark        # Use custom executable"
            echo "  $0 --runs 3                           # 3 runs"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            print_info "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Run main function
main "$@"
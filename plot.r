#!/usr/bin/env Rscript
# --- 1. Setup: Load Required Packages ---
required_packages <- c("ggplot2", "dplyr", "readr", "tidyr", "scales", "RColorBrewer", "purrr")

for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    install.packages(pkg, repos = "https://cran.r-project.org/")
    library(pkg, character.only = TRUE)
  }
}


# --- 2. Data Loading and Preprocessing ---
load_and_prepare_data <- function(file_path) {
  if (!file.exists(file_path)) stop("Performance data file not found: ", file_path)
  cat("Loading data from:", file_path, "\n")

  tryCatch({
    data <- read_csv(file_path, show_col_types = FALSE, guess_max = 5000)
  }, error = function(e) {
    stop("Error reading CSV file: ", e$message)
  })

  if (nrow(data) == 0) stop("CSV file contains no data.")

  required_cols <- c("execution_policy", "num_processes", "num_threads", "chunk_size", "record_count", "completion_time_ms")
  if (!all(required_cols %in% colnames(data))) {
    stop("Missing required columns. Found: ", paste(colnames(data), collapse=", "))
  }

  data %>%
    mutate(
      execution_policy = factor(execution_policy, levels = c("OMP", "FastFlow", "MPI_FF")),
      num_processes = as.numeric(as.character(num_processes)),
      num_threads = as.numeric(as.character(num_threads)),
      chunk_size_num = as.numeric(as.character(chunk_size)),
      record_count_num = as.numeric(as.character(record_count)),
      
      chunk_size_label = factor(
        case_when(
          chunk_size_num >= 1e6 ~ paste0(chunk_size_num / 1e6, "M"),
          chunk_size_num >= 1e3 ~ paste0(chunk_size_num / 1e3, "K"),
          TRUE ~ as.character(chunk_size_num)
        ), 
        levels = unique(
          case_when(
            sort(unique(chunk_size_num)) >= 1e6 ~ paste0(sort(unique(chunk_size_num)) / 1e6, "M"),
            sort(unique(chunk_size_num)) >= 1e3 ~ paste0(sort(unique(chunk_size_num)) / 1e3, "K"),
            TRUE ~ as.character(sort(unique(chunk_size_num)))
          )
        )
      ),
      
      record_count_label = factor(
        case_when(
          record_count_num >= 1e6 ~ paste0(record_count_num / 1e6, "M"),
          record_count_num >= 1e3 ~ paste0(record_count_num / 1e3, "K"),
          TRUE ~ as.character(record_count_num)
        ), 
        levels = unique(
          case_when(
            sort(unique(record_count_num)) >= 1e6 ~ paste0(sort(unique(record_count_num)) / 1e6, "M"),
            sort(unique(record_count_num)) >= 1e3 ~ paste0(sort(unique(record_count_num)) / 1e3, "K"),
            TRUE ~ as.character(sort(unique(record_count_num)))
          )
        )
      )
    ) %>%
    select(-chunk_size_num, -record_count_num) %>%
    filter(completion_time_ms > 0, !is.na(completion_time_ms), !is.infinite(completion_time_ms))
}

# --- 3. Plotting Functions ---

# Helper function to create human-readable subtitle
create_subtitle <- function(combo) {
  param_names <- list(
    "record_count_label" = "Record Count",
    "chunk_size_label" = "Chunk Size", 
    "num_processes" = "Processes",
    "num_threads" = "Threads"
  )
  
  subtitle_parts <- c()
  for (col_name in names(combo)) {
    if (col_name %in% names(param_names)) {
      human_name <- param_names[[col_name]]
      value <- combo[[col_name]]
      subtitle_parts <- c(subtitle_parts, paste(human_name, "=", value))
    }
  }
  
  if (length(subtitle_parts) > 0) {
    return(paste("Fixed:", paste(subtitle_parts, collapse = ", ")))
  } else {
    return("")
  }
}

# Plot 1: Side-by-Side Policy Comparison (with Error Bars)
generate_policy_comparison_plots <- function(data, param_to_vary, fixed_params, combo, timestamp, output_dir) {
  subtitle_text <- create_subtitle(combo)
  
  # Handle different policies based on their natural configurations
  plot_data_list <- list()
  
  if (param_to_vary %in% c("chunk_size_label", "record_count_label")) {
    # For chunk size and record count comparisons, we want to compare policies
    # but each policy should use its own optimal/representative configuration
    
    # For OMP and FastFlow: always use num_processes = 1
    omp_ff_data <- data %>%
      filter(execution_policy %in% c("OMP", "FastFlow"), num_processes == 1)
    
    # For MPI_FF: use all available configurations, but we'll take a representative one
    # or aggregate across process/thread combinations
    mpi_data <- data %>%
      filter(execution_policy == "MPI_FF")
    
    # Filter by the fixed parameters from combo (excluding process/thread constraints)
    fixed_params_no_workers <- intersect(names(combo), c("chunk_size_label", "record_count_label"))
    
    if (length(fixed_params_no_workers) > 0) {
      omp_ff_data <- omp_ff_data %>%
        inner_join(combo %>% select(all_of(fixed_params_no_workers)), by = fixed_params_no_workers)
      mpi_data <- mpi_data %>%
        inner_join(combo %>% select(all_of(fixed_params_no_workers)), by = fixed_params_no_workers)
    }
    
    plot_data_list <- list(omp_ff_data, mpi_data)
    
  } else if (param_to_vary == "num_threads") {
    # When varying threads, handle each policy appropriately
    
    # For OMP and FastFlow: num_processes = 1, vary num_threads
    omp_ff_data <- data %>%
      filter(execution_policy %in% c("OMP", "FastFlow"), num_processes == 1)
    
    # For MPI_FF: use a fixed number of processes (from combo if available)
    mpi_processes <- if ("num_processes" %in% names(combo)) combo$num_processes else 2
    mpi_data <- data %>%
      filter(execution_policy == "MPI_FF", num_processes == mpi_processes)
    
    # Apply other fixed parameters
    other_fixed <- intersect(names(combo), c("chunk_size_label", "record_count_label"))
    if (length(other_fixed) > 0) {
      omp_ff_data <- omp_ff_data %>%
        inner_join(combo %>% select(all_of(other_fixed)), by = other_fixed)
      mpi_data <- mpi_data %>%
        inner_join(combo %>% select(all_of(other_fixed)), by = other_fixed)
    }
    
    plot_data_list <- list(omp_ff_data, mpi_data)
    
  } else if (param_to_vary == "num_processes") {
    # When varying processes, only MPI_FF should vary, others stay at 1
    
    # For OMP and FastFlow: always num_processes = 1
    omp_ff_data <- data %>%
      filter(execution_policy %in% c("OMP", "FastFlow"), num_processes == 1)
    
    # For MPI_FF: vary num_processes, use fixed threads if specified
    mpi_data <- data %>%
      filter(execution_policy == "MPI_FF")
    
    if ("num_threads" %in% names(combo)) {
      mpi_data <- mpi_data %>% filter(num_threads == combo$num_threads)
    }
    
    # Apply other fixed parameters
    other_fixed <- intersect(names(combo), c("chunk_size_label", "record_count_label"))
    if (length(other_fixed) > 0) {
      omp_ff_data <- omp_ff_data %>%
        inner_join(combo %>% select(all_of(other_fixed)), by = other_fixed)
      mpi_data <- mpi_data %>%
        inner_join(combo %>% select(all_of(other_fixed)), by = other_fixed)
    }
    
    plot_data_list <- list(omp_ff_data, mpi_data)
  }
  
  # Combine all data
  plot_data <- bind_rows(plot_data_list) %>%
    group_by(execution_policy, .data[[param_to_vary]]) %>%
    summarise(
      mean_time = mean(completion_time_ms),
      se = sd(completion_time_ms) / sqrt(n()),
      n_observations = n(),
      .groups = "drop"
    )
    
  if (nrow(plot_data) < 1) {
    cat("No data for combination:", paste(names(combo), combo, sep="=", collapse=", "), "\n")
    return()
  }
  
  # Check if we have data for all three policies
  policies_present <- unique(plot_data$execution_policy)
  cat("Policies present in plot data:", paste(policies_present, collapse = ", "), 
      "| Total data points:", sum(plot_data$n_observations), "\n")

  param_display_names <- list(
    "num_processes" = "Number of Processes",
    "num_threads" = "Number of Threads", 
    "chunk_size_label" = "Chunk Size",
    "record_count_label" = "Record Count"
  )
  
  x_label <- if (param_to_vary %in% names(param_display_names)) {
    param_display_names[[param_to_vary]]
  } else {
    param_to_vary
  }

  p <- ggplot(plot_data, aes(x = factor(.data[[param_to_vary]]), y = mean_time, fill = execution_policy)) +
    geom_bar(stat = "identity", position = position_dodge(width = 0.9)) +
    geom_errorbar(aes(ymin = pmax(0, mean_time - se), ymax = mean_time + se), width = 0.25, position = position_dodge(0.9)) +
    scale_fill_brewer(palette = "Set2") +
    labs(
      title = paste("Policy Comparison vs.", x_label),
      subtitle = subtitle_text,
      x = x_label,
      y = "Mean Completion Time (ms)",
      fill = "Execution Policy"
    ) +
    theme_minimal(base_size = 14) +
    theme(
      plot.title = element_text(hjust = 0.5, face = "bold"),
      plot.subtitle = element_text(hjust = 0.5, size = 10),
      legend.position = "bottom",
      axis.text.x = element_text(angle = 45, hjust = 1)
    )

  filename_suffix <- paste(sapply(names(combo), function(cn) paste0(substr(cn, 1, 1), combo[[cn]])), collapse="_")
  filename <- file.path(output_dir, sprintf("comparison_%s_%s_%s.png", param_to_vary, filename_suffix, timestamp))
  
  cat("Saving plot:", filename, "\n")
  ggsave(filename, p, width = 11, height = 8, dpi = 300)
}

# Plot 2: Combined Speedup and Efficiency (Fixed)
generate_combined_scalability_plots <- function(data, fixed_params_combo, timestamp, output_dir) {
  subtitle_text <- create_subtitle(fixed_params_combo)

  plot_data <- data %>% inner_join(fixed_params_combo, by = names(fixed_params_combo))

  if (nrow(plot_data) == 0) {
    cat("No data for scalability combination:", paste(names(fixed_params_combo), fixed_params_combo, sep="=", collapse=", "), "\n")
    return()
  }
  
  # Calculate speedup for each policy
  omp_ff_data <- plot_data %>%
    filter(execution_policy %in% c("OMP", "FastFlow"), num_processes == 1) %>%
    group_by(execution_policy) %>%
    mutate(baseline_time = mean(completion_time_ms[num_threads == min(num_threads)])) %>%
    group_by(execution_policy, num_threads) %>%
    summarise(mean_time = mean(completion_time_ms), baseline_time = first(baseline_time), .groups="drop") %>%
    mutate(workers = num_threads, speedup = baseline_time / mean_time)

  mpi_data <- plot_data %>%
    filter(execution_policy == "MPI_FF") %>%
    mutate(workers = num_processes * num_threads) %>%
    group_by(execution_policy) %>%
    mutate(baseline_time = mean(completion_time_ms[workers == min(workers)])) %>%
    group_by(execution_policy, workers) %>%
    summarise(mean_time = mean(completion_time_ms), baseline_time = first(baseline_time), .groups="drop") %>%
    mutate(speedup = baseline_time / mean_time)
  
  combined_data <- bind_rows(omp_ff_data, mpi_data) %>%
    mutate(efficiency = speedup / workers * 100)

  if (nrow(combined_data) < 2) {
    cat("Insufficient data for scalability plots\n")
    return()
  }
  
  # Speedup Plot
  p_speedup <- ggplot(combined_data, aes(x = workers, y = speedup, color = execution_policy, group = execution_policy)) +
    geom_line(size = 1) + geom_point(size = 3) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "black", alpha = 0.7) +
    scale_color_brewer(palette = "Set2") +
    labs(
      title = "Combined Speedup Analysis", 
      subtitle = subtitle_text, 
      x = "Number of Workers/Threads", 
      y = "Speedup",
      color = "Execution Policy"
    ) +
    theme_minimal(base_size = 14) +
    theme(
      legend.position = "bottom", 
      plot.title = element_text(hjust = 0.5, face = "bold"), 
      plot.subtitle = element_text(hjust = 0.5, size = 10)
    )

  filename_suffix <- paste(sapply(names(fixed_params_combo), function(cn) paste0(substr(cn, 1, 1), fixed_params_combo[[cn]])), collapse="_")
  filename <- file.path(output_dir, sprintf("speedup_combined_%s_%s.png", filename_suffix, timestamp))
  
  cat("Saving speedup plot:", filename, "\n")
  ggsave(filename, p_speedup, width = 10, height = 7, dpi = 300)

  # Efficiency Plot
  p_efficiency <- ggplot(combined_data, aes(x = workers, y = efficiency, color = execution_policy, group = execution_policy)) +
    geom_line(size = 1) + geom_point(size = 3) +
    geom_hline(yintercept = 100, linetype = "dashed", color = "black", alpha = 0.7) +
    scale_color_brewer(palette = "Set2") +
    labs(
      title = "Combined Efficiency Analysis", 
      subtitle = subtitle_text, 
      x = "Number of Workers/Threads", 
      y = "Efficiency (%)",
      color = "Execution Policy"
    ) +
    theme_minimal(base_size = 14) +
    theme(
      legend.position = "bottom", 
      plot.title = element_text(hjust = 0.5, face = "bold"), 
      plot.subtitle = element_text(hjust = 0.5, size = 10)
    )
  
  filename <- file.path(output_dir, sprintf("efficiency_combined_%s_%s.png", filename_suffix, timestamp))
  cat("Saving efficiency plot:", filename, "\n")
  ggsave(filename, p_efficiency, width = 10, height = 7, dpi = 300)
}

# --- 4. Main Execution Logic ---
run_analysis <- function(file_path, timestamp, output_dir) {
  
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
  
  data <- load_and_prepare_data(file_path)
  
  cat("Data summary:\n")
  cat("Execution policies:", paste(unique(data$execution_policy), collapse = ", "), "\n")
  cat("Number of processes:", paste(sort(unique(data$num_processes)), collapse = ", "), "\n")
  cat("Number of threads:", paste(sort(unique(data$num_threads)), collapse = ", "), "\n")
  cat("Chunk sizes:", paste(unique(data$chunk_size_label), collapse = ", "), "\n")
  cat("Record counts:", paste(unique(data$record_count_label), collapse = ", "), "\n")
  cat("Total rows:", nrow(data), "\n\n")
  
  # --- Generate Policy Comparison Plots ---
  params_to_vary <- c("num_processes", "num_threads", "chunk_size_label", "record_count_label")
  
  for (param in params_to_vary) {
    cat(paste("### Generating Policy Comparison plots | Varying:", param, "###\n"))
    
    if (param %in% c("chunk_size_label", "record_count_label")) {
      # For chunk size and record count, create plots that compare all policies
      # We'll create one plot per combination of the OTHER parameter
      other_param <- setdiff(c("chunk_size_label", "record_count_label"), param)
      combinations <- data %>% distinct(!!sym(other_param))
      
    } else if (param == "num_threads") {
      # For thread comparison, fix chunk_size and record_count, 
      # and use representative process counts
      combinations <- data %>% 
        distinct(chunk_size_label, record_count_label)
        
    } else if (param == "num_processes") {
      # For process comparison, fix other parameters
      combinations <- data %>% distinct(chunk_size_label, record_count_label, num_threads)
    }
    
    cat("Found", nrow(combinations), "combinations for parameter:", param, "\n")
    
    # Generate plots for each combination
    for (i in 1:nrow(combinations)) {
      combo <- combinations[i, , drop = FALSE]
      cat("Processing combination", i, "of", nrow(combinations), ":", 
          paste(names(combo), combo, sep="=", collapse=", "), "\n")
      
      generate_policy_comparison_plots(
        data = data, 
        param_to_vary = param, 
        fixed_params = NULL,  # We handle this inside the function now
        combo = combo, 
        timestamp = timestamp, 
        output_dir = output_dir
      )
    }
    cat("\n")
  }
  
  # --- Generate Combined Speedup and Efficiency Plots ---
  cat("### Generating Combined Speedup/Efficiency plots ###\n")
  # These plots vary workers, so we fix chunk and record size
  scalability_combinations <- data %>% distinct(chunk_size_label, record_count_label)
  cat("Found", nrow(scalability_combinations), "combinations for scalability plots\n")
  
  for (i in 1:nrow(scalability_combinations)) {
    combo <- scalability_combinations[i, , drop = FALSE]
    cat("Processing scalability combination", i, "of", nrow(scalability_combinations), "\n")
    
    generate_combined_scalability_plots(
      data = data, 
      fixed_params_combo = combo,
      timestamp = timestamp, 
      output_dir = output_dir
    )
  }
  
  cat("\nAnalysis complete. All plots saved to:", output_dir, "\n")
}

# --- 5. Command Line Interface ---
if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  
  if (length(args) < 1) {
    cat("Usage: Rscript plot.r <csv_file> [timestamp] [output_dir]\n")
    quit(status = 1)
  }
  
  file_path <- args[1]
  timestamp <- if (length(args) >= 2) args[2] else format(Sys.time(), "%Y-%m-%d_%H%M")
  output_dir <- if (length(args) >= 3) args[3] else "performance_plots"
  
  run_analysis(file_path, timestamp, output_dir)
}
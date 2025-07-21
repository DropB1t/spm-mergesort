#!/usr/bin/env Rscript

# Out-of-core MergeSort Performance Analysis Script
# Generates histogram plots for completion times across different parameters

# Required packages
required_packages <- c("ggplot2", "dplyr", "readr", "tidyr", "scales", "gridExtra", "viridis", "RColorBrewer", "grid")

# Install and load packages
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    install.packages(pkg, repos = "https://cran.r-project.org/")
    library(pkg, character.only = TRUE)
  }
}

# Function to load and process mergesort performance data
load_data <- function(file_path) {
  if (!file.exists(file_path)) {
    stop("Performance data file not found: ", file_path)
  }
  
  cat("Loading data from:", file_path, "\n")
  
  # Read CSV with error handling
  tryCatch({
    data <- read_csv(file_path, show_col_types = FALSE)
  }, error = function(e) {
    stop("Error reading CSV file: ", e$message)
  })
  
  # Check if data is empty
  if (nrow(data) == 0) {
    stop("CSV file contains no data")
  }
  
  # Check for required columns
  required_cols <- c("execution_policy", "num_processes", "num_threads", "chunk_size", "record_count", "completion_time_ms")
  missing_cols <- setdiff(required_cols, colnames(data))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }
  
  data <- data %>%
    mutate(
      # Convert to factors for proper ordering
      execution_policy = factor(execution_policy, levels = c("OMP", "FastFlow", "MPI_FF")),
      num_processes = factor(num_processes, levels = sort(unique(num_processes))),
      num_threads = factor(num_threads, levels = sort(unique(num_threads))),
      chunk_size = factor(chunk_size, levels = sort(unique(chunk_size))),
      record_count = factor(record_count, levels = sort(unique(record_count))),
      
      # Convert chunk size to readable format using vectorized operations
      chunk_size_num = as.numeric(as.character(chunk_size)),
      chunk_size_label = case_when(
        chunk_size_num == 5000 ~ "5K",
        chunk_size_num == 50000 ~ "50K",
        chunk_size_num == 100000 ~ "100K",
        chunk_size_num == 1000000 ~ "1M",
        chunk_size_num >= 1000000 ~ paste0(chunk_size_num/1000000, "M"),
        chunk_size_num >= 1000 ~ paste0(chunk_size_num/1000, "K"),
        TRUE ~ as.character(chunk_size_num)
      ),
      chunk_size_label = factor(chunk_size_label, levels = unique(chunk_size_label[order(chunk_size_num)])),
      
      # Convert record count to readable format using vectorized operations
      record_count_num = as.numeric(as.character(record_count)),
      record_count_label = case_when(
        record_count_num == 1000000 ~ "1M",
        record_count_num == 5000000 ~ "5M",
        record_count_num == 10000000 ~ "10M",
        record_count_num >= 1000000 ~ paste0(record_count_num/1000000, "M"),
        record_count_num >= 1000 ~ paste0(record_count_num/1000, "K"),
        TRUE ~ as.character(record_count_num)
      ),
      record_count_label = factor(record_count_label, levels = unique(record_count_label[order(record_count_num)]))
    ) %>%
    select(-chunk_size_num, -record_count_num) %>%  # Remove temporary columns
    filter(
      completion_time_ms > 0,
      !is.na(completion_time_ms),
      !is.infinite(completion_time_ms)
    )
  
  cat("Loaded", nrow(data), "measurements\n")
  cat("Execution policies:", paste(levels(data$execution_policy), collapse = ", "), "\n")
  cat("Number of processes:", paste(levels(data$num_processes), collapse = ", "), "\n")
  cat("Number of threads:", paste(levels(data$num_threads), collapse = ", "), "\n")
  cat("Chunk sizes:", paste(levels(data$chunk_size_label), collapse = ", "), "\n")
  cat("Record counts:", paste(levels(data$record_count_label), collapse = ", "), "\n")
  
  return(data)
}

plot_mean_completion_by_processes <- function(data, current_param_set = "") {
  avg_data <- data %>%
    group_by(execution_policy, num_processes) %>%
    summarise(
      mean_time = mean(completion_time_ms),
      se_time = sd(completion_time_ms) / sqrt(n()),
      .groups = "drop"
    )
  
  p <- avg_data %>%
    ggplot(aes(x = num_processes, y = mean_time, fill = execution_policy)) +
    geom_col(position = position_dodge(width = 0.8), alpha = 0.8, color = "white", linewidth = 0.5) +
    geom_errorbar(aes(ymin = pmax(0, mean_time - se_time), ymax = mean_time + se_time),
                  position = position_dodge(width = 0.8), width = 0.25, alpha = 0.7) +
    scale_fill_manual(values = c("OMP" = "#ff7f0e", "FastFlow" = "#1f77b4", "MPI_FF" = "#2ca02c")) +
    scale_y_continuous(labels = comma_format()) +
    labs(
      title = paste("Mean Completion Time by Number of Processes", current_param_set),
      x = "Number of Processes",
      y = "Mean Completion Time (ms)",
      fill = "Execution Policy"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 12),
      legend.position = "bottom"
    )
  
  return(p)
}

plot_mean_completion_by_threads <- function(data, current_param_set = "") {
  avg_data <- data %>%
    group_by(execution_policy, num_threads) %>%
    summarise(
      mean_time = mean(completion_time_ms),
      se_time = sd(completion_time_ms) / sqrt(n()),
      .groups = "drop"
    )
  
  p <- avg_data %>%
    ggplot(aes(x = num_threads, y = mean_time, fill = execution_policy)) +
    geom_col(position = position_dodge(width = 0.8), alpha = 0.8, color = "white", linewidth = 0.5) +
    geom_errorbar(aes(ymin = pmax(0, mean_time - se_time), ymax = mean_time + se_time),
                  position = position_dodge(width = 0.8), width = 0.25, alpha = 0.7) +
    scale_fill_manual(values = c("OMP" = "#ff7f0e", "FastFlow" = "#1f77b4", "MPI_FF" = "#2ca02c")) +
    scale_y_continuous(labels = comma_format()) +
    labs(
      title = paste("Mean Completion Time by Number of Threads", current_param_set),
      x = "Number of Threads",
      y = "Mean Completion Time (ms)",
      fill = "Execution Policy"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 12),
      legend.position = "bottom"
    )
  
  return(p)
}

plot_mean_completion_by_chunk_size <- function(data, current_param_set = "") {
  avg_data <- data %>%
    group_by(execution_policy, chunk_size_label) %>%
    summarise(
      mean_time = mean(completion_time_ms),
      se_time = sd(completion_time_ms) / sqrt(n()),
      .groups = "drop"
    )
  
  p <- avg_data %>%
    ggplot(aes(x = chunk_size_label, y = mean_time, fill = execution_policy)) +
    geom_col(position = position_dodge(width = 0.8), alpha = 0.8, color = "white", linewidth = 0.5) +
    geom_errorbar(aes(ymin = pmax(0, mean_time - se_time), ymax = mean_time + se_time),
                  position = position_dodge(width = 0.8), width = 0.25, alpha = 0.7) +
    scale_fill_manual(values = c("OMP" = "#ff7f0e", "FastFlow" = "#1f77b4", "MPI_FF" = "#2ca02c")) +
    scale_y_continuous(labels = comma_format()) +
    labs(
      title = paste("Mean Completion Time by Chunk Size", current_param_set),
      x = "Chunk Size",
      y = "Mean Completion Time (ms)",
      fill = "Execution Policy"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 12),
      legend.position = "bottom"
    )
  
  return(p)
}

plot_mean_completion_by_record_count <- function(data, current_param_set = "") {
  avg_data <- data %>%
    group_by(execution_policy, record_count_label) %>%
    summarise(
      mean_time = mean(completion_time_ms),
      se_time = sd(completion_time_ms) / sqrt(n()),
      .groups = "drop"
    )
  
  p <- avg_data %>%
    ggplot(aes(x = record_count_label, y = mean_time, fill = execution_policy)) +
    geom_col(position = position_dodge(width = 0.8), alpha = 0.8, color = "white", linewidth = 0.5) +
    geom_errorbar(aes(ymin = pmax(0, mean_time - se_time), ymax = mean_time + se_time),
                  position = position_dodge(width = 0.8), width = 0.25, alpha = 0.7) +
    scale_fill_manual(values = c("OMP" = "#ff7f0e", "FastFlow" = "#1f77b4", "MPI_FF" = "#2ca02c")) +
    scale_y_continuous(labels = comma_format()) +
    labs(
      title = paste("Mean Completion Time by Record Count", current_param_set),
      x = "Record Count",
      y = "Mean Completion Time (ms)",
      fill = "Execution Policy"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 12),
      legend.position = "bottom"
    )
  
  return(p)
}

# Function to create comprehensive summary plot
create_summary_plot <- function(data, current_param_set = "") {
  p1 <- plot_mean_completion_by_processes(data, "") + theme(legend.position = "none")
  p2 <- plot_mean_completion_by_threads(data, "") + theme(legend.position = "none")  
  p3 <- plot_mean_completion_by_chunk_size(data, "") + theme(legend.position = "none")
  p4 <- plot_mean_completion_by_record_count(data, "")
  
  # Extract legend from p4
  legend <- get_legend(p4)
  p4 <- p4 + theme(legend.position = "none")
  
  # Combine plots
  combined <- arrangeGrob(
    arrangeGrob(p1, p2, p3, p4, ncol = 2),
    legend,
    heights = c(8, 1),
    top = textGrob(paste("Performance Summary Plot", current_param_set), 
                   gp = gpar(fontsize = 15))
  )
  
  return(combined)
}

# Helper function to extract legend
get_legend <- function(plot) {
  tmp <- ggplot_gtable(ggplot_build(plot))
  leg <- which(sapply(tmp$grobs, function(x) x$name) == "guide-box")
  legend <- tmp$grobs[[leg]]
  return(legend)
}

# Main analysis function
run_analysis <- function(file_path, timestamp, output_dir = "performance_plots", current_param_set = "") {
  # Create output directory with error handling
  tryCatch({
    if (!dir.exists(output_dir)) {
      dir.create(output_dir, recursive = TRUE)
      cat("Created output directory:", output_dir, "\n")
    }
    
    # Test write permissions
    test_file <- file.path(output_dir, "test_write.tmp")
    writeLines("test", test_file)
    file.remove(test_file)
    cat("Output directory is writable:", output_dir, "\n")
    
  }, error = function(e) {
    stop("Cannot create or write to output directory '", output_dir, "': ", e$message)
  })
  
  # Load and process data
  data <- load_data(file_path)
  
  # Generate individual plots
  cat("\nGenerating histogram plots...\n")
  
  # Create plots with error handling
  tryCatch({
    p_processes_mean <- plot_mean_completion_by_processes(data, current_param_set)
    ggsave(file.path(output_dir, paste0("mean_by_processes_", timestamp, ".png")), 
           p_processes_mean, width = 10, height = 6, dpi = 300)
    cat("Saved: mean_by_processes_", timestamp, ".png\n")
    
    p_threads_mean <- plot_mean_completion_by_threads(data, current_param_set)
    ggsave(file.path(output_dir, paste0("mean_by_threads_", timestamp, ".png")), 
           p_threads_mean, width = 10, height = 6, dpi = 300)
    cat("Saved: mean_by_threads_", timestamp, ".png\n")
    
    p_chunk_mean <- plot_mean_completion_by_chunk_size(data, current_param_set)
    ggsave(file.path(output_dir, paste0("mean_by_chunk_size_", timestamp, ".png")), 
           p_chunk_mean, width = 10, height = 6, dpi = 300)
    cat("Saved: mean_by_chunk_size_", timestamp, ".png\n")
    
    p_records_mean <- plot_mean_completion_by_record_count(data, current_param_set)
    ggsave(file.path(output_dir, paste0("mean_by_record_count_", timestamp, ".png")), 
           p_records_mean, width = 10, height = 6, dpi = 300)
    cat("Saved: mean_by_record_count_", timestamp, ".png\n")
    
    # Summary plot
    p_summary <- create_summary_plot(data, current_param_set)
    ggsave(file.path(output_dir, paste0("performance_summary_", timestamp, ".png")), 
           p_summary, width = 12, height = 10, dpi = 300)
    cat("Saved: performance_summary_", timestamp, ".png\n")
    
  }, error = function(e) {
    stop("Error saving plots: ", e$message)
  })
  
  cat("Plots saved to directory:", output_dir, "\n")
  cat("Analysis complete!\n\n")
  
  return(list(data = data))
}

# Command line interface
if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  
  if (length(args) < 1) {
    cat("Usage: Rscript ms_plot_script.r <csv_file> <timestamp> [output_dir] [param_set_description]\n")
    quit(status = 1)
  }
  
  file_path <- args[1]
  timestamp <- if (length(args) >= 2) args[2] else format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
  output_dir <- if (length(args) >= 3) args[3] else "performance_plots"  
  current_param_set <- if (length(args) >= 4) args[4] else ""
  
  # Run the analysis
  results <- run_analysis(file_path, timestamp, output_dir, current_param_set)
}

# For interactive use:
# results <- run_analysis("ms_test.csv", format(Sys.time(), "%Y-%m-%d_%H-%M-%S"), "performance_plots", "Test Run")
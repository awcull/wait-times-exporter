package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// Config holds database connection parameters
type Config struct {
	Host      string
	Port      int
	User      string
	Password  string
	DBName    string
	SSLMode   string
	OutputDir string
}

// loadConfig loads configuration from environment variables
func loadConfig() (Config, error) {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		return Config{}, fmt.Errorf("error loading .env file: %v", err)
	}

	// Parse port as integer
	port, err := strconv.Atoi(os.Getenv("DB_PORT"))
	if err != nil {
		return Config{}, fmt.Errorf("invalid DB_PORT: %v", err)
	}

	// Create config from environment variables
	config := Config{
		Host:      getEnvWithDefault("DB_HOST", "localhost"),
		Port:      port,
		User:      getEnvWithDefault("DB_USER", "postgres"),
		Password:  getEnvWithDefault("DB_PASSWORD", ""),
		DBName:    getEnvWithDefault("DB_NAME", "hospital_db"),
		SSLMode:   getEnvWithDefault("DB_SSLMODE", "disable"),
		OutputDir: getEnvWithDefault("OUTPUT_DIR", "data_exports"),
	}

	return config, nil
}

// getEnvWithDefault returns the value of an environment variable or a default value if not set
func getEnvWithDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func main() {
	// Load configuration from .env file
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("Error loading configuration: %v\n", err)
		return
	}

	// Connect to the database
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.DBName, config.SSLMode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		return
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		fmt.Printf("Error pinging database: %v\n", err)
		return
	}
	fmt.Println("Successfully connected to the database")

	// Create output directory
	err = os.MkdirAll(config.OutputDir, 0755)
	if err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		return
	}

	// Current date for commit message and JSON data
	currentDate := time.Now().Format("2006-01-02")

	// Views to query
	views := []string{
		"hospital_seven_avg_change",
		"daily_wait_time_stats",
		"monthly_avg_wait_times",
	}

	// Query each view and save to JSON
	for _, view := range views {
		// Query the view
		var jsonData []byte
		query := fmt.Sprintf("SELECT json_agg(t) FROM (SELECT * FROM %s) t", view)

		err = db.QueryRow(query).Scan(&jsonData)
		if err != nil {
			fmt.Printf("Error querying view %s: %v\n", view, err)
			continue
		}

		// If null result, create an empty array
		if jsonData == nil {
			jsonData = []byte("[]")
		}

		// Add export date field
		jsonData, err = addExportDateToJSON(jsonData, currentDate)
		if err != nil {
			fmt.Printf("Error adding date to JSON for %s: %v\n", view, err)
			continue
		}

		// Format the JSON for better readability
		var prettyJSON bytes.Buffer
		err = json.Indent(&prettyJSON, jsonData, "", "  ")
		if err != nil {
			fmt.Printf("Error formatting JSON for %s: %v\n", view, err)
			continue
		}

		// Save to file
		filename := filepath.Join(config.OutputDir, fmt.Sprintf("%s.json", view))
		err = os.WriteFile(filename, prettyJSON.Bytes(), 0644)
		if err != nil {
			fmt.Printf("Error writing file %s: %v\n", filename, err)
			continue
		}

		fmt.Printf("Successfully exported %s to %s\n", view, filename)
	}

	// Git operations
	if err := gitCommitAndPush(config.OutputDir, currentDate); err != nil {
		fmt.Printf("Error with Git operations: %v\n", err)
		return
	}

	fmt.Println("Data export and Git operations completed successfully")
}

// addExportDateToJSON adds an export_date field to the JSON data
func addExportDateToJSON(data []byte, date string) ([]byte, error) {
	// Create a wrapper object that contains the data and export date
	wrapper := map[string]interface{}{
		"data":        json.RawMessage(data),
		"export_date": date,
	}

	// Convert the wrapper back to JSON
	return json.Marshal(wrapper)
}

func gitCommitAndPush(repoPath, commitMessage string) error {
	// Change to the repository directory
	currentDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current directory: %v", err)
	}

	// Change to the repository directory
	if err := os.Chdir(repoPath); err != nil {
		return fmt.Errorf("failed to change directory: %v", err)
	}
	defer os.Chdir(currentDir) // Return to original directory when function completes

	// Add all files
	cmd := exec.Command("git", "add", ".")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git add failed: %v, output: %s", err, output)
	}
	fmt.Println("Added files to Git staging area")

	// Commit with date as message
	commitMsg := fmt.Sprintf("Data export %s", commitMessage)
	cmd = exec.Command("git", "commit", "-m", commitMsg)
	if output, err := cmd.CombinedOutput(); err != nil {
		// Check if it's just "nothing to commit" error
		outStr := string(output)
		if !bytes.Contains(output, []byte("nothing to commit")) {
			return fmt.Errorf("git commit failed: %v, output: %s", err, outStr)
		}
		fmt.Println("Nothing to commit, working tree clean")
		return nil
	}
	fmt.Printf("Committed changes with message: %s\n", commitMsg)

	// Push to remote repository
	cmd = exec.Command("git", "push")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git push failed: %v, output: %s", err, output)
	}
	fmt.Println("Pushed changes to remote repository")

	return nil
}

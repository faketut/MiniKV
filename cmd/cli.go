package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"minikv/kv"
)

func main() {
	config := kv.DefaultConfig("./data")
	db, err := kv.NewKV(config)
	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("MiniKV CLI - Type 'help' for commands")
	fmt.Print("> ")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print("> ")
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			fmt.Print("> ")
			continue
		}

		command := strings.ToLower(parts[0])

		switch command {
		case "put":
			if len(parts) != 3 {
				fmt.Println("Usage: put <key> <value>")
			} else {
				err := db.Put([]byte(parts[1]), []byte(parts[2]))
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("OK")
				}
			}

		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
			} else {
				value, found, err := db.Get([]byte(parts[1]))
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else if found {
					fmt.Println(string(value))
				} else {
					fmt.Println("not found")
				}
			}

		case "delete":
			if len(parts) != 2 {
				fmt.Println("Usage: delete <key>")
			} else {
				err := db.Delete([]byte(parts[1]))
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("OK")
				}
			}

		case "help":
			fmt.Println("Commands:")
			fmt.Println("  put <key> <value>  - Store a key-value pair")
			fmt.Println("  get <key>          - Retrieve a value by key")
			fmt.Println("  delete <key>       - Delete a key")
			fmt.Println("  exit               - Exit the CLI")
			fmt.Println("  quit               - Exit the CLI")

		case "exit", "quit":
			fmt.Println("Goodbye!")
			return

		default:
			fmt.Printf("Unknown command: %s. Type 'help' for commands.\n", command)
		}

		fmt.Print("> ")
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}


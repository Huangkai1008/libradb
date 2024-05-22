# LibraDB

[![Build](https://github.com/Huangkai1008/libradb/workflows/Test/badge.svg)](https://github.com/Huangkai1008/libradb/actions/workflows/tests.yaml)
[![codecov](https://codecov.io/gh/Huangkai1008/libradb/branch/master/graph/badge.svg)](https://codecov.io/gh/Huangkai1008/libradb)
[![Go Report Card](https://goreportcard.com/badge/github.com/Huangkai1008/libradb)](https://goreportcard.com/report/github.com/Huangkai1008/libradb)
[![GoDoc](https://godoc.org/github.com/Huangkai1008/libradb?status.svg)](https://godoc.org/github.com/Huangkai1008/libradb)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://www.mit-license.org/)

LibraDB is a Go-based relational database project. It allows for the creation, querying, updating, and deletion of
records in a structured manner. The database operations are performed using SQL commands and managed through a Go
interface.

## Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [License](#license)

## Requirements

- Go 1.22 or later

## Installation

1. Install [Go](https://github.com/golang/go) and set your workspace

2. Get the project

```bash
go -get https://github.com/Huangkai1008/libradb
```

## Usage

1.Add the libs

```bash
go mod download       
```

2.Run the application

```bash
go run cmd/main.go
```

## License

[MIT @ Huang Kai](./LICENSE)


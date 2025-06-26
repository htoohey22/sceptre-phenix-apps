# minimega (kafka) Component
Collects and filters desired Kafka data into CSV/ JSON format, so that Kafka data can be easily used by programs such as Excel. The output directory for the data is listed in the phenix log when the scorch component starts.

```
type: kafka
exe:  phenix-scorch-component-kafka
```
## Metadata Options

```yaml
metadata:
    bootstrapServers: [<string>] #IP_address:port_number sending Kafka data
    csv: <bool> #boolean indicating if the output should be a csv, if false we return a JSON file
    topics: [([(key, value)]], name)] #a list containing all topics to subscribe to and key value pairs to filter by (see yaml example for formatting)

```
## Example Configuration

```yaml
- metadata:
    bootstrap Servers:
      - 1.0.0.0:9092 #Kafka uses port 9092
    csv: false
    topics:
      - filter:
          - key: foo
            value: bar
          - key: foo2
            value: bar2
        name: {{BRANCH_NAME}}.foo.bar
      - filter:
          - key: foo3
            value: bar3
        name: {{BRANCH_NAME}}.foo2.bar2
```

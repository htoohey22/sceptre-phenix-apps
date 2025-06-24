## Metadata Options

```yaml
metadata:
    bootstrapServers: [<string>]
    allScan: <bool> #boolean indicating if all tags should be scanned or if specific tags should be subscribed to
    subscribeTags: <list> #list of strings that indiate which tags we will subscribe to
    mode: <string>  #string that can be 'substation', 'critical load', or 'all data'
    #'substation' mode means we will collect data from a general substation by scanning the data name field for a keyword using wildcards
    #'critical load' mode means we will only collect data from a specific critical load like 'load_p3ulv67354_1'
    #'all data' mode means that we will collect all data from the tags we are subscribed to
    critLoad: <string> #string indicated critical load to scan for
    substation: <string> #name of substation to scan for
    csv: <bool> #boolean indicating if the output should be a csv, if false we return a JSON file
```
## Example Configuration

```yaml
- metadata:
        components:
          - metadata:
              allScan: false
              bootstrap Servers:
                - 172.20.0.74:9092
              critLoad: ''
              subscribeTags:
                - 'bytrerage.power.load'
              csv: true
              mode: all data
              substation: ''
            name: foobar
            type: kafka
        runs:
          - start:
              - foobar
      name: scorch
```

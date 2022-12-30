# logql

```
package "github.com/commentlens/loghouse/api/loki/logql"

Query : LogQuery | MetricQuery;

var_name : ( upcase | lowcase | '_' ) { upcase | lowcase | '_' | number };
string : ( '"' { not "\"" } '"' | '`' { not "`" } '`' );
duration : '[' { not "]" } ']';
float : [ '-' ]
          ( any "123456789" { number } | '0' )
          [ '.' { number } ]
          [ any "eE" [ any "+-" ] { number } ]
        ;

LogQuery : LogSelector PipelinesMaybe;
LogSelector : "{" LogSelectorMembersMaybe "}";
LogSelectorMembersMaybe : empty | LogSelectorMembers;
LogSelectorMembers : LogSelectorMember | LogSelectorMember "," LogSelectorMembers;
LogSelectorMember : LabelKey LogSelectorOp string;
LabelKey : var_name;
LogSelectorOp : "=" | "!=" | "=~" | "!~";

PipelinesMaybe : empty | Pipelines;
Pipelines : Pipeline | Pipeline Pipelines;
Pipeline : LineFilter | LabelFilter;
LineFilter : LineFilterOp string;
LineFilterOp : "|=" | "!=" | "|~" | "!~";
LabelFilter : "|" NestedLabelKey LabelFilterOp LabelFilterValue;
NestedLabelKey : var_name | var_name "." NestedLabelKey;
LabelFilterOp : "=" | "!=" | "=~" | "!~" | ">=" | ">" | "<=" | "<";
LabelFilterValue : string | float;

MetricQuery : "sum" "by" "(" "level" ")" "(" "count_over_time" "(" LogQuery duration ")" ")";
```

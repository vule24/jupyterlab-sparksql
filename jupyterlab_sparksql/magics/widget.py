from pathlib import Path
from datetime import datetime
import IPython.display as ipd
import ipywidgets as w
import pandas as pd
import copy, re, json

from pyspark.sql import functions as F
pd.options.plotting.backend = "plotly"

TABLE_STYLES = [
    {"selector": '', 'props': [
        ('border-bottom', '1px solid var(--jp-border-color1)'),
    ]},
    {'selector': 'th', 'props': 'white-space: pre;'},
    {'selector': 'td', 'props': 'white-space: pre;'},
    {'selector': 'thead th', 'props': [
        ('position', 'sticky'), 
        ('top', '0'), 
        ('z-index', '1'),
        ('background-color', 'var(--jp-layout-color0)'),
        ('border-right', 'var(--jp-border-width) solid var(--jp-border-color1) !important'),
        ('border-bottom', 'var(--jp-border-width) solid var(--jp-border-color1) !important'),
        ('border-top', 'var(--jp-border-width) solid var(--jp-border-color1) !important'),
        ('cursor', 'pointer')
    ]},
    {'selector': 'thead th:first-child', 'props': [
        ('position', 'sticky'), 
        ('left', '0'), 
        ('z-index', '2'), 
        ('background-color', 'var(--jp-layout-color0)'),
        ('border-left', 'var(--jp-border-width) solid var(--jp-border-color1) !important')
    ]},
    {'selector': 'tbody th', 'props': [
        ('position', 'sticky'), 
        ('left', '0'), 
        ('z-index', '1'), 
        ('background-color', 'inherit'),
        ('border-left', 'var(--jp-border-width) solid var(--jp-border-color1) !important'),
        ('border-right', 'var(--jp-border-width) solid var(--jp-border-color1) !important'),
    ]},
    {'selector': 'tbody td', 'props': [
        ('border-right', 'var(--jp-border-width) solid var(--jp-border-color1) !important'),
    ]}
]
TABLE_ATTRIBUTES = 'style="border-collapse:separate"'
TABLE_STYLE_FORMAT = dict(na_rep='null', precision=3, thousands=",", decimal=".")
AGG_FULL_LIST = ['-', 'Count', 'CountDistinct', 'Min', 'Max', 'Sum', 'Avg']
AGG_LIMIT_LIST = ['-', 'Count', 'CountDistinct', 'Min', 'Max']
AGG_MAP = {
    '-': '-',
    'Count': 'count',
    'CountDistinct': 'nunique',
    'Min': 'min',
    'Max': 'max',
    'Sum': 'sum',
    'Avg': 'mean'
}

def cast_decimal_to_float(sdf):
    for c in sdf.columns:
        if 'DecimalType' in str(sdf.schema[c].dataType):
            sdf = sdf.withColumn(c, F.col(c).cast('float'))
    return sdf
     
def extract_table_id(table_html):
    match = re.search(r'<table.*?id="(.*?)".*?>', table_html)
    if match:
        return match.group(1)

def pd_dtypes_to_js_types(df):
    dtype_map = {
        'int8': 'Number',
        'int16': 'Number',
        'int32': 'Number',
        'int64': 'Number',
        'uint8': 'Number',
        'uint16': 'Number',
        'uint32': 'Number',
        'uint64': 'Number',
        'float8': 'Number',
        'float16': 'Number',
        'float32': 'Number',
        'float64': 'Number',
        'timedelta64[ns]': 'Number',
        'bool': 'Boolean',
        'datetime64[ns]': 'Date'
    }
    col_types = []
    for col in df.columns:
        if str(df[col].dtype) in dtype_map:
            col_types.append(dtype_map[str(df[col].dtype)])
        else:
            col_types.append('String')
    return json.dumps(['Number', *col_types])


def generate_table(sdf, num_rows):
    with pd.option_context(
        'display.max_rows', None, 
        'display.max_columns', None, 
        'display.max_colwidth', None,
    ):
        dataframe = copy.deepcopy(cast_decimal_to_float(sdf.limit(num_rows)).toPandas())
        dataframe.index = dataframe.index + 1

        table_html = ("<div style='max-height: 650px'>"
                        + dataframe.style
                            .format(**TABLE_STYLE_FORMAT)
                            .set_table_styles(TABLE_STYLES)
                            .set_table_attributes(TABLE_ATTRIBUTES)
                            .to_html(notebook=True)
                        + "</div>")
        
        table_id = extract_table_id(table_html)

        # add sortTable to every column header
        # Find the thead tag
        thead = re.search(r'<thead>[\s\S]*?</thead>', table_html)
        if thead:
            # Extract the content of the thead tag
            thead_content = thead.group()
            # Find all th tags within the thead tag
            th_tags = re.findall(r'<th.*?>.*?</th>', thead_content)
        for i, tag in enumerate(th_tags):
            tag_open = re.findall(r'<th[^e][^>]*>', tag)[0]
            tag_content = re.findall(r'(?<=>)(.*)(?=</th>)', tag)[0] if i != 0 else '#'
            new_tag = tag_open[:-1] + f' onclick="(function(){{ sortTable(\'{table_id}\', {i}) }})()">' + tag_content + ' <i class="fa fa-sort" style="color: var(--jp-inverse-layout-color3)"></i></th>'
            table_html = table_html.replace(tag, new_tag)

        table_html = f"""<script>
            function parseType(value, n) {{
                let type = {pd_dtypes_to_js_types(dataframe)}[n];
                switch (type) {{
                    case 'Number':
                        if (value === 'inf') return Number.POSITIVE_INFINITY
                        else if (value === '-inf') return Number.NEGATIVE_INFINITY
                        else return Number(value);
                    case 'Boolean':
                        return value === 'true';
                    case 'Date':
                        return Date.parse(value);
                    default:
                        return value;
                }}
            }}
            function sortTable(table_id, n) {{
                let table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
                table = document.getElementById(table_id);
                switching = true;
                dir = "asc";
                while (switching) {{
                    switching = false;
                    rows = table.getElementsByTagName("tr");
                    for (i = 1; i < rows.length - 1; i++) {{
                        shouldSwitch = false;
                        x = rows[i].querySelectorAll("th, td")[n].innerHTML.toLowerCase();
                        y = rows[i + 1].querySelectorAll("th, td")[n].innerHTML.toLowerCase();
                        console.log('x=',x,'y=', y, {pd_dtypes_to_js_types(dataframe)}[n]);
                        if (x == "null") {{
                            shouldSwitch = true;
                            break;
                        }}
                        x = parseType(x, n);
                        y = parseType(y, n);
                        
                        if (dir == "asc") {{
                            console.log('x > y: ', x>y);
                            if (x > y) {{
                                shouldSwitch = true;
                                break;
                            }}
                        }} else if (dir == "desc") {{
                            if (x < y) {{
                                shouldSwitch = true;
                                break;
                            }}
                        }}
                        console.log('x=',x,'y=', y);
                    }}
                    if (shouldSwitch) {{
                        rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                        switching = true;
                        switchcount++;
                    }} else {{
                        if (switchcount == 0 && dir == "asc") {{
                            dir = "desc";
                            switching = true;
                        }}
                    }}
                    console.log(shouldSwitch);
                }}
                let headers = table.getElementsByTagName("thead")[0].getElementsByTagName("th");
                for (let header_idx = 0; header_idx < headers.length; header_idx++) {{
                    headers[header_idx].innerHTML = headers[header_idx].innerHTML.replace(/<i[^>]*>[\s\S]*?<\/i>/g, '<i class="fa fa-sort" style="color: var(--jp-inverse-layout-color3)"></i>')
                    if (header_idx == n) {{
                        if (dir == "asc") headers[header_idx].innerHTML = headers[header_idx].innerHTML.replace(/<i[^>]*>[\s\S]*?<\/i>/g, '<i class="fa fa-sort-asc" style="color: var(--jp-brand-color1)"></i>')
                        else if (dir == "desc") headers[header_idx].innerHTML = headers[header_idx].innerHTML.replace(/<i[^>]*>[\s\S]*?<\/i>/g, '<i class="fa fa-sort-desc" style="color: var(--jp-brand-color1)"></i>')
                    }}
                }}
                
            }}
        </script>""" + table_html

        input_search = f"""
        <div class="lm-Widget p-Widget jupyter-widgets widget-inline-hbox widget-text" style="margin: 1px 2px 2px 2px; width: 150px;">
            <input type="text" id="inputSearch_{table_id}" placeholder="Search" onkeyup="(function () {{
                let filter = this.value.toUpperCase();
                let tbody = document.getElementById('{table_id}').getElementsByTagName('tbody')[0];
                let rows = tbody.getElementsByTagName('tr');
                for (let i=0; i < rows.length; i++) {{
                    let cols = rows[i].getElementsByTagName('td');
                    if (cols.length === 0) continue;
                    let rowIsMatched = false;
                    for (let j=0; j < cols.length; j++) {{
                        const cellContent = cols[j].textContent || cols[j].innerText;
                        if (cellContent.toUpperCase().indexOf(filter) > -1) {{
                            rowIsMatched = true;
                            break;
                        }}
                    }}
                    if (rowIsMatched) {{
                        rows[i].style.display = '';
                    }} else {{
                        rows[i].style.display = 'none';
                    }}
                }}
            }}).call(this)">
        </div>
        """
        
        return (
            dataframe,
            table_html,
            input_search
        )

def plot(current_render, template, dataframe, x, y, agg, logx, logy):
    plot_df = None
    plot_y = None

    pandas_agg = AGG_MAP.get(agg)

    if pandas_agg == '-':
        plot_df = dataframe
        plot_y = y
    else:
        if x != y:
            plot_df = dataframe.groupby(x).agg({y: pandas_agg}).reset_index().rename(columns={y: f'{agg}( {y} )'})
            plot_y = f'{agg}( {y} )'
        else:
            plot_df = dataframe
            plot_df[f'{agg}( {y} )'] = plot_df[y]
            plot_df = plot_df.groupby(x).agg({f'{agg}( {y} )': pandas_agg}).reset_index()
            plot_y = f'{agg}( {y} )'
    
    fig = plot_df.plot(
        kind=current_render, 
        x=x,
        y=plot_y, 
        template=template,
    )
    fig.update_layout(height=650)

    if logx:
        fig.update_layout(xaxis_type="log")
    if logy:
        fig.update_layout(yaxis_type="log")
 
    return fig


def generate_output_widget(sdf, num_rows, export_table_name=None):
    dataframe, table_html, input_search = generate_table(sdf, num_rows=num_rows)
    state = dict(
        current_render='table',
        template=None,
        fig=None
    )
    column_options = dataframe.columns.to_list()
    
    try:
        theme_file = Path.home()/r'.jupyter/lab/user-settings/@jupyterlab/apputils-extension/themes.jupyterlab-settings'
        lines = theme_file.read_text().split('\n')
        for line in lines:
            if '"theme"' in line.strip():
                if "JupyterLab Light" in line.strip():
                    state['template'] = None
                else:
                    state['template'] = 'plotly_dark'
    except Exception as err:
        state['template'] = None
    
    
    # Elements
    layout_btn_render_type = w.Layout(
        width='50px',
        margin='1px 2px 2px 2px')
 
    btn_table = w.Button(
        disabled=False,
        button_style='',
        icon='table',
        layout=layout_btn_render_type)
    btn_chart_line = w.Button(
        disabled=False,
        button_style='',
        icon='chart-line',
        layout=layout_btn_render_type)
    btn_chart_bar = w.Button(
        disabled=False,
        button_style='',
        icon='chart-bar',
        layout=layout_btn_render_type)
    btn_chart_scatter = w.Button(
        disabled=False,
        button_style='',
        icon='chart-scatter',
        layout=layout_btn_render_type)
    btn_save_csv = w.Button(
        description='Save as CSV',
        disabled=False,
        button_style='warning',
        icon='save',
        layout=w.Layout(width='auto', margin='1px 2px 2px 2px'))
    btn_submit_widget = w.Button(
        disabled=False,
        button_style='primary',
        icon='thumb-tack',
        layout=layout_btn_render_type)
    dropdown_x = w.Dropdown(
        value=column_options[0],
        options=column_options,
        description='X:',
        layout=w.Layout(width='max-content', max_width='120px', margin='1px 10px 2px 2px'),
        style={'description_width': 'initial'})
    dropdown_y = w.Dropdown(
        value=column_options[1],
        options=column_options,
        description='Y:',
        layout=w.Layout(width='max-content', max_width='120px', margin='1px 10px 2px 2px'),
        style={'description_width': 'initial'})
    dropdown_aggregation = w.Dropdown(
        options=AGG_FULL_LIST,
        description='Agg:',
        layout=w.Layout(width='max-content', margin='1px 10px 2px 2px'),
        style={'description_width': 'initial'})
    checkbox_logx = w.Checkbox(
        value=False,
        disabled=False,
        indent=False,
        layout=w.Layout(width='max-content', margin='1px 2px 2px 2px', padding='2px 0 0 0'))
    checkbox_logy = w.Checkbox(
        value=False,
        disabled=False,
        indent=False,
        layout=w.Layout(width='max-content', margin='1px 2px 2px 2px', padding='2px 0 0 0'))

    
    # Layout
    viz_types = w.HBox(
        children=[
            btn_table, 
            btn_chart_line, 
            btn_chart_bar, 
            btn_chart_scatter,
            btn_submit_widget
        ], 
        layout=w.Layout(align_items='center')
    )
    tbl_console_box = w.HBox([w.HTML(input_search), btn_save_csv], layout=w.Layout(display='flex', align_items='center', justify_content='flex-end', flex_flow='row wrap'))
    viz_console_box = w.HBox(
        [
            w.HBox([dropdown_x, dropdown_y, dropdown_aggregation]),
            w.HBox([w.Label('LogX:', layout=w.Layout(margin='0 5px 2px 2px')), checkbox_logx, w.Label('LogY:', layout=w.Layout(margin='0 5px 2px 2px')), checkbox_logy]),
        ], 
        layout=w.Layout(display='flex', justify_content='flex-end', flex_flow='row wrap')
    )
    console = w.Output(
        layout=w.Layout(
            display='flex', 
            align_items='center', 
            margin='0px'
        )
    )
    viz_output = w.Output()
    output = w.Output()

    with output:
        ipd.display(
            w.VBox(
                [
                    w.HBox(
                        [viz_types, console], 
                        layout=w.Layout(
                            display='flex',
                            justify_content='space-between',
                            align_items='center',
                            flex_flow='row wrap'
                        )
                    ),
                    viz_output
                ],
                layout=w.Layout(width='100%')
            )
        )

    # Event
    
    def on_btn_save_csv_clicked(b):
        b.icon = 'spinner spin'
        b.button_style = 'info'
        b.description = 'Saving'
        b.disabled = True
        
        output_dir = Path.cwd() / 'data-export'
        output_dir.mkdir(parents=True, exist_ok=True)
        output_filename = output_dir / f'{"" if not export_table_name else export_table_name + "_" }{datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}.csv'
        dataframe.to_csv(output_filename, index=False)
        
        b.icon = 'check'
        b.button_style = ''
        b.description = 'Saved'
        b.disabled = False
        b.tooltip = str(output_filename.resolve())
        
    btn_save_csv.on_click(on_btn_save_csv_clicked)
    
    def on_btn_render_clicked(b):
        for btn in [btn_table, btn_chart_line, btn_chart_bar, btn_chart_scatter]:
            btn.button_style=''
        b.button_style = 'warning'
        
        if b.icon == 'table':
            with console:
                ipd.clear_output()
                ipd.display(tbl_console_box)
            with viz_output:
                ipd.clear_output(wait=True)
                ipd.display(ipd.HTML(table_html))
        else:
            state['current_render'] = b.icon.split('-')[-1]
            state['fig'] = plot(
                current_render=state['current_render'],
                template=state['template'],
                dataframe=dataframe,
                x=dropdown_x.value,
                y=dropdown_y.value,
                agg=dropdown_aggregation.value,
                logx=checkbox_logx.value,
                logy=checkbox_logy.value
            )
            with console:
                ipd.clear_output(wait=True)
                ipd.display(viz_console_box)
            with viz_output:
                ipd.clear_output(wait=True)
                ipd.display(state['fig'])
            
    btn_table.on_click(on_btn_render_clicked)
    btn_chart_line.on_click(on_btn_render_clicked)
    btn_chart_bar.on_click(on_btn_render_clicked)
    btn_chart_scatter.on_click(on_btn_render_clicked)
    
    def on_console_change(change):
        state['fig'] = plot(
            current_render=state['current_render'],
            template=state['template'],
            dataframe=dataframe,
            x=dropdown_x.value,
            y=dropdown_y.value,
            agg=dropdown_aggregation.value,
            logx=checkbox_logx.value,
            logy=checkbox_logy.value
        )
        with viz_output:
            ipd.clear_output(wait=True)
            ipd.display(state['fig'])
    
    dropdown_x.observe(on_console_change, 'value')
    dropdown_y.observe(on_console_change, 'value')
    dropdown_aggregation.observe(on_console_change, 'value')
    checkbox_logx.observe(on_console_change, 'value')
    checkbox_logy.observe(on_console_change, 'value')

    def on_btn_submit_clicked(b):
        with output:
            ipd.clear_output(wait=True)
            if state['current_render'] == 'table':
                ipd.display(ipd.HTML(input_search))
                ipd.display(ipd.HTML(table_html))
            else:
                ipd.clear_output(wait=True)
                ipd.display(state['fig'])
    btn_submit_widget.on_click(on_btn_submit_clicked)

    
    def on_dropdown_y_selected(change):
        if change['new'] == change['old']:
            return
        if change['new'] in dataframe.select_dtypes(include=['object', 'datetime', 'timedelta', 'datetimetz']).columns.to_list():
            dropdown_aggregation.options = AGG_LIMIT_LIST
        else:
            dropdown_aggregation.options = AGG_FULL_LIST
        dropdown_aggregation.value = AGG_FULL_LIST[0]
    
    dropdown_y.observe(on_dropdown_y_selected, 'value')

    # render
    btn_table.click()

    return output

from pathlib import Path
from datetime import datetime
import IPython.display as ipd
import ipywidgets as w
import pandas as pd
import numpy as np
import copy
pd.options.plotting.backend = "plotly"

def generate_classic_table(sdf, num_rows):
     with pd.option_context(
        'display.max_rows', None, 
        'display.max_columns', None, 
        'display.max_colwidth', None
    ):
        return ipd.HTML(
            "<div style='max-height: 650px'>" +
            sdf.limit(num_rows).toPandas().style
                .format(na_rep='null', precision=3, thousands=",", decimal=".")
                .set_table_styles([
                    {'selector': 'thead th', 'props': 'position: sticky; top: 0; z-index: 1; background-color: var(--jp-layout-color0); border-bottom: var(--jp-border-width) solid var(--jp-border-color1) !important;'},
                    {'selector': 'thead th:first-child', 'props': 'position: sticky; left: 0; z-index: 2; background-color: var(--jp-layout-color0);'},
                    {'selector': 'tbody th', 'props': 'position: sticky; left: 0; z-index: 1; background-color: inherit;'},
                ])
                .set_table_attributes(
                    'style="border-collapse:separate"'
                )
                .to_html() +
            "</div>"
        )

def generate_table(sdf, num_rows):
    with pd.option_context(
        'display.max_rows', None, 
        'display.max_columns', None, 
        'display.max_colwidth', None
    ):
        dataframe = copy.deepcopy(sdf.limit(num_rows).toPandas())
        return (
            dataframe,
            ipd.HTML(
                "<div style='max-height: 650px'>" +
                dataframe.style
                    .format(na_rep='null', precision=3, thousands=",", decimal=".")
                    .set_table_styles([
                        {'selector': 'thead th', 'props': 'position: sticky; top: 0; z-index: 1; background-color: var(--jp-layout-color0); border-bottom: var(--jp-border-width) solid var(--jp-border-color1) !important;'},
                        {'selector': 'thead th:first-child', 'props': 'position: sticky; left: 0; z-index: 2; background-color: var(--jp-layout-color0);'},
                        {'selector': 'tbody th', 'props': 'position: sticky; left: 0; z-index: 1; background-color: inherit;'},
                    ])
                    .set_table_attributes(
                        'style="border-collapse:separate"'
                    )
                    .to_html() +
                "</div>"
            )
        )

def plot(output_widget, current_render, template, dataframe, x, y, agg, logx, logy):
    plot_df = None
    plot_y = None
    if agg == '-':
        plot_df = dataframe
        plot_y = y
    else:
        if x != y:
            plot_df = dataframe.groupby(x).agg({y: agg}).reset_index().rename(columns={y: f'{agg}_{y}'})
            plot_y = f'{agg}_{y}'
        else:
            plot_df = dataframe
            plot_df[f'{agg}_{y}'] = plot_df[y]
            plot_df = plot_df.groupby(x).agg({f'{agg}_{y}': agg}).reset_index()
            plot_y = f'{agg}_{y}'
        
 
    with output_widget:
        ipd.clear_output(wait=True)
        fig = plot_df.plot(
            kind=current_render, 
            x=x,
            y=plot_y, 
            template=template,
        )
        if logx:
            fig.update_layout(xaxis_type="log")
        if logy:
            fig.update_layout(yaxis_type="log")
        ipd.display(fig)


def generate_output_widget(sdf, num_rows, export_table_name=None):
    dataframe, table_html = generate_table(sdf, num_rows=num_rows)
    state = dict(
        current_render='table',
        template=None
    )
    
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
    
    
    # elements
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
    dropdown_x = w.Dropdown(
        options=dataframe.columns,
        description='X:',
        layout=w.Layout(width='max-content', max_width='120px', margin='1px 10px 2px 2px'),
        style={'description_width': 'initial'})
    dropdown_y = w.Dropdown(
        options=dataframe.select_dtypes(include=np.number).columns.to_list(),
        description='Y:',
        layout=w.Layout(width='max-content', max_width='120px', margin='1px 10px 2px 2px'),
        style={'description_width': 'initial'})
    dropdown_aggregation = w.Dropdown(
        options=['-','min', 'max', 'count', 'sum', 'mean'],
        description='Agg:',
        layout=w.Layout(width='max-content', max_width='120px', margin='1px 10px 2px 2px'),
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

    
    # layout
    output_types = w.HBox(
        children=[
            btn_table, 
            btn_chart_line, 
            btn_chart_bar, 
            btn_chart_scatter
        ], 
        layout=w.Layout(align_items='center'))
    
    console_box = w.HBox([
        w.HBox([dropdown_x, dropdown_y, dropdown_aggregation]),
        w.HBox([w.Label('LogX:', layout=w.Layout(margin='0 5px 2px 2px')), checkbox_logx, w.Label('LogY:', layout=w.Layout(margin='0 5px 2px 2px')), checkbox_logy]),
    ])
    console = w.Output(
        layout=w.Layout(
            display='flex', 
            align_items='center', 
            margin='0px'))
    
    output = w.Output(style="width: 100%; padding: 0px")
    
    
    
    # event
    
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
            with output:
                ipd.clear_output(wait=True)
                ipd.display(table_html) 
        else:
            state['current_render'] = b.icon.split('-')[-1]
            with console:
                ipd.clear_output(wait=True)
                ipd.display(console_box)
            plot(
                output, 
                current_render=state['current_render'],
                template=state['template'],
                dataframe=dataframe,
                x=dropdown_x.value,
                y=dropdown_y.value,
                agg=dropdown_aggregation.value,
                logx=checkbox_logx.value,
                logy=checkbox_logy.value
            )
    btn_table.on_click(on_btn_render_clicked)
    btn_chart_line.on_click(on_btn_render_clicked)
    btn_chart_bar.on_click(on_btn_render_clicked)
    btn_chart_scatter.on_click(on_btn_render_clicked)
    
    def on_console_change(change):
        plot(
            output, 
            current_render=state['current_render'],
            template=state['template'],
            dataframe=dataframe,
            x=dropdown_x.value,
            y=dropdown_y.value,
            agg=dropdown_aggregation.value,
            logx=checkbox_logx.value,
            logy=checkbox_logy.value
        )
    dropdown_x.observe(on_console_change, 'value')
    dropdown_y.observe(on_console_change, 'value')
    dropdown_aggregation.observe(on_console_change, 'value')
    checkbox_logx.observe(on_console_change, 'value')
    checkbox_logy.observe(on_console_change, 'value')
    
        
    
    # render
    btn_table.click()

    return w.VBox([
        w.HBox(
            [output_types, w.HBox([console]), btn_save_csv], 
            layout=w.Layout(
                display='flex',
                justify_content='space-between',
                align_items='center',
            )
        ),
        output
    ])

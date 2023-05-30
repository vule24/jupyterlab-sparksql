import IPython.display as ipd
from .sparksql import SparkSQL


def load_ipython_extension(ipython):
    ipd.display_javascript(
        "try {"
            "require(['notebook/js/codecell'], function (codecell) {"
                "codecell.CodeCell.options_default.highlight_modes['magic_text/x-pgsql'] = { 'reg': [/^%%sql/, /^.*spark\.sql\(/] };"
                "Jupyter.notebook.events.one('kernel_ready.Kernel', function () {"
                    "Jupyter.notebook.get_cells().map(function (cell) {"
                        "if (cell.cell_type == 'code') { cell.auto_highlight(); }"
                    "});"
                "});"
            "});"
        "} catch(e) {}"
    , raw=True)
    ipd.display(ipd.Javascript("""
        var sparkSqlWidgetIconUpdateEventId = localStorage.getItem('sparkSqlWidgetIconUpdateEventId');
        if (sparkSqlWidgetIconUpdateEventId !== null) {
            clearInterval(sparkSqlWidgetIconUpdateEventId)
        }
        sparkSqlWidgetIconUpdateEventId = setInterval(function () {
            document.querySelectorAll(".fa-chart-scatter").forEach(element => {
                let scatterIcon = `<span>
                                        <svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="14px" height="10px" viewBox="0 0 13 10" version="1.1">
                                            <g id="surface1">
                                                <path fill="currentColor" d="M 0.472656 8.242188 L 12.527344 8.242188 C 12.777344 8.242188 12.980469 8.453125 12.980469 8.714844 L 12.980469 9.460938 C 12.980469 9.722656 12.777344 9.9375 12.527344 9.9375 L 0.472656 9.9375 C 0.222656 9.9375 0.0195312 9.722656 0.0195312 9.460938 L 0.0195312 8.714844 C 0.0195312 8.453125 0.222656 8.242188 0.472656 8.242188 Z M 0.472656 8.242188 "/>
                                                <path fill="currentColor" d="M 0.472656 0 L 1.1875 0 C 1.4375 0 1.640625 0.214844 1.640625 0.476562 L 1.640625 9.460938 C 1.640625 9.722656 1.4375 9.9375 1.1875 9.9375 L 0.472656 9.9375 C 0.222656 9.9375 0.0195312 9.722656 0.0195312 9.460938 L 0.0195312 0.476562 C 0.0195312 0.214844 0.222656 0 0.472656 0 Z M 0.472656 0 "/>
                                                <path fill="currentColor" d="M 5.285156 5.488281 C 5.285156 4.902344 4.832031 4.429688 4.273438 4.429688 C 3.714844 4.429688 3.261719 4.902344 3.261719 5.488281 C 3.261719 6.074219 3.714844 6.546875 4.273438 6.546875 C 4.832031 6.546875 5.285156 6.074219 5.285156 5.488281 Z M 5.285156 5.488281 "/>
                                                <path fill="currentColor" d="M 7.308594 2.308594 C 7.308594 1.722656 6.855469 1.25 6.296875 1.25 C 5.738281 1.25 5.285156 1.722656 5.285156 2.308594 C 5.285156 2.894531 5.738281 3.367188 6.296875 3.367188 C 6.855469 3.367188 7.308594 2.894531 7.308594 2.308594 Z M 7.308594 2.308594 "/>
                                                <path fill="currentColor" d="M 11.359375 1.886719 C 11.359375 1.300781 10.90625 0.828125 10.347656 0.828125 C 9.789062 0.828125 9.335938 1.300781 9.335938 1.886719 C 9.335938 2.46875 9.789062 2.945312 10.347656 2.945312 C 10.90625 2.945312 11.359375 2.46875 11.359375 1.886719 Z M 11.359375 1.886719 "/>
                                                <path fill="currentColor" d="M 9.335938 4.96875 C 9.335938 4.382812 8.882812 3.910156 8.324219 3.910156 C 7.761719 3.910156 7.308594 4.382812 7.308594 4.96875 C 7.308594 5.554688 7.761719 6.027344 8.324219 6.027344 C 8.882812 6.027344 9.335938 5.554688 9.335938 4.96875 Z M 9.335938 4.96875 "/>
                                            </g>
                                        </svg>
                                    </span>`;
                element.parentNode.innerHTML = scatterIcon;
            });
        }, 50)

        localStorage.setItem('sparkSqlWidgetIconUpdateEventId', sparkSqlWidgetIconUpdateEventId)
    """))
    ipd.display(ipd.HTML("<style>.jp-OutputArea-output { width: 10% !important }</style>"))
    ipython.register_magics(SparkSQL)
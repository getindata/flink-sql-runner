from typing import Dict

from jinja2 import Environment, FileSystemLoader


class JinjaTemplateResolver(object):
    def resolve(
            self,
            template_dir: str,
            template_file: str,
            vars: Dict[str, str],
            output_file_path: str,
    ) -> None:
        environment = Environment(loader=FileSystemLoader(template_dir))
        template = environment.get_template(template_file)
        content = template.render(**vars)
        with open(output_file_path, mode="w", encoding="utf-8") as run_file:
            run_file.truncate()
            run_file.write(content)

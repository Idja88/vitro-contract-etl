# vitro-contract-etl

## Описание
Проект представляет собой ETL-процесс на языке Python, который загружает и трансформирует финансовые данные из системы 1C в виде Excel-файлов в базу данных MS SQL Server, для дальнейшего построения отчетов на платформе SSRS.

## Настройка

В корневой папке проекта необходимо создать `config.json` со следующей структурой:

```json
{
    "file_paths" : "",
    "table_names" : [""],
    "connection_string" : "",
    "mail_message": {
        "from_email": "your@mail.com",
        "to_emails": ["your@mail.com","your2@email.com"],
        "smtp_server": "mail.server.com",
        "smtp_port": 25
    }
}
```
from datetime import datetime
import io
import os
from zipfile import ZipFile
import logging

logger = logging.getLogger(__file__)


def create_and_packaging_zip(report_id):
    """
    Упаковка скриншотов в архив, чистка дирректории screenshots
    :param report_id:
    :return:
    """
    if not report_id:
        print('Ошибка формирования архива со скриншотами объявлений.')
        raise ValueError('Ошибка формирования архива со скриншотами объявлений. Не удалось получить report_id')

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    output_zip = io.BytesIO()
    output_zip.name = f'{report_id}/скриншоты_объявлений_{timestamp}.zip'
    with ZipFile(output_zip, mode='w') as zipfile:
        for screen in os.scandir('screenshots'):
            # упаковываем изображение в архив
            zipfile.write(screen.path, screen.path.split(os.sep)[-1])
            # удаляем изображение из папки
            os.remove(screen.path)
    output_zip.seek(0)
    return output_zip


def html_remove():
    """
    Чиста дирректории от HTML-файлов
    :return:
    """
    logger.info('Удаление html-файлов...')
    dirname = os.path.dirname(__file__)
    for file in os.scandir(dirname):
        if file.name.endswith('.html'):
            os.remove(file.path)
            print('Файл ', file.path, ' удалён.')

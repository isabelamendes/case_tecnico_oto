from abc import ABC, abstractmethod
from pathlib import Path
import logging
import pandas as pd
import chardet
import time


class ChunkProcessor(ABC):
    """
    Classe base genérica para processar arquivos em chunks.
    Subclasses devem implementar a função process, definindo como ler e salvar os chunks.
    """
    def __init__(self, input_file, output_file=None, chunk_size: int = 1000,
                 processing_funcs=None):
        self.input_file = Path(input_file)
        self.output_file = Path(output_file) if output_file else self._default_output_file()
        self.chunk_size = chunk_size

        if processing_funcs is None:
            self.processing_funcs = []
        elif callable(processing_funcs):
            self.processing_funcs = [processing_funcs]
        else:
            self.processing_funcs = processing_funcs

        self.log_file = self.input_file.with_name(f"{self.input_file.stem}_processing.log")
        self._setup_logger()

    def _setup_logger(self):
        self.logger = logging.getLogger(f"ChunkProcessor_{self.input_file.stem}")
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        fh = logging.FileHandler(self.log_file, mode='w', encoding='utf-8')
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def _default_output_file(self) -> Path:
        """
        Retorna arquivo de saída padrão (mesma extensão do input).
        """
        return self.input_file.with_name(f"{self.input_file.stem}_processed{self.input_file.suffix}")

    def apply_processing_funcs(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica todas as funções de processamento no chunk.
        """
        for func in self.processing_funcs:
            chunk = func(chunk)
        return chunk

    @abstractmethod
    def read_chunks(self):
        """
        Gera chunks do arquivo. Deve ser implementado pela subclasse.
        """
        pass
            
    @abstractmethod
    def process(self):
        """
        Processa o arquivo em chunks.
        Deve ser implementado pelas subclasses.
        """
        pass


class ChunkProcessorCsv(ChunkProcessor):
    """
    Processador de arquivos CSV em chunks.
    """
    def detect_encoding(self, sample_size=10000) -> str:
        with open(self.input_file, 'rb') as f:
            raw_data = f.read(sample_size)
        result = chardet.detect(raw_data)
        self.logger.info(f"Encoding detectado: {result['encoding']}")
        return result['encoding']

    def read_chunks(self):
        """
        Lê o CSV em chunks.
        """
        encoding = self.detect_encoding()
        self.logger.info(f"Lendo CSV com encoding: {encoding}")
        reader = pd.read_csv(self.input_file, chunksize=self.chunk_size, encoding=encoding)
        for i, chunk in enumerate(reader, 1):
            self.logger.info(f"Lido chunk {i} com {len(chunk)} linhas")
            yield chunk

    def process(self):
        """
        Processa o CSV em chunks, salvando incrementalmente no arquivo de saída.
        """
        start_time_total = time.time()
        self.logger.info(f"Iniciando processamento do arquivo: {self.input_file}")

        chunk_number = 0
        total_rows = 0
        total_nulls = 0

        with open(self.output_file, 'w', encoding='utf-8', newline='') as f_out:
            header_written = False
            for chunk in self.read_chunks():
                chunk_number += 1
                start_time_chunk = time.time()
                try:
                    chunk = self.apply_processing_funcs(chunk)
                    # Salva o chunk no CSV
                    chunk.to_csv(f_out, index=False, header=not header_written)
                    header_written = True

                    num_rows = len(chunk)
                    num_nulls = chunk.isnull().sum().sum()
                    total_rows += num_rows
                    total_nulls += num_nulls
                    elapsed_chunk = time.time() - start_time_chunk

                    self.logger.info(f"Chunk {chunk_number} processado: {num_rows} linhas, "
                                     f"tempo: {elapsed_chunk:.2f}s, linhas nulas: {num_nulls}")
                except Exception as e:
                    self.logger.error(f"Erro no chunk {chunk_number}: {e}")

        elapsed_total = time.time() - start_time_total
        self.logger.info(f"Processamento concluído. Arquivo salvo em: {self.output_file}")
        self.logger.info(f"Total de chunks processados: {chunk_number}")
        self.logger.info(f"Total de linhas processadas: {total_rows}")
        self.logger.info(f"Total de valores nulos encontrados: {total_nulls}")
        self.logger.info(f"Tempo total de processamento: {elapsed_total:.2f} segundos")

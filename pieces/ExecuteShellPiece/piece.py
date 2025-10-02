from domino.base_piece import BasePiece
from .models import InputModel, OutputModel
import subprocess
from pathlib import Path


class ExecuteShellPiece(BasePiece):

    def piece_function(self, input_data: InputModel):

        # process = subprocess.Popen(['/usr/bin/ping', '-c 4', 'python.org'],
        process = subprocess.Popen(['/usr/bin/netstat', '-tulpn'],
                                   stdout=subprocess.PIPE,
                                   universal_newlines=True)

        all_output = ""

        while True:
            output = process.stdout.readline()
            all_output = all_output + output
            print(output.strip())
            # Do something else
            return_code = process.poll()
            if return_code is not None:
                print('RETURN CODE', return_code)
                # Process has finished, read rest of the output
                for output in process.stdout.readlines():
                    all_output = all_output + output
                    print(output.strip())
                break

        message = f"Netstat executed successfully"
        file_path = str(Path(self.results_path) / "output.txt")
        with open(file_path, "a") as f:
            f.write(all_output)
        f.close()

        # Return output
        return OutputModel(
            message=message,
            file_path=file_path
        )

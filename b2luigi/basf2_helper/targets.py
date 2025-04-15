import b2luigi


class ROOTLocalTarget(b2luigi.LocalTarget):
    class ROOTLocalTarget:
        """
        A custom target class that extends :class:`b2luigi.LocalTarget` to handle ROOT files.

        This class overrides the ``exists`` method to not only check for the existence
        of the file but also verify that the ROOT file contains at least one key.
        """

    def exists(self):
        """
        Checks if the target file exists and contains at least one key.

        This method first verifies the existence of the file using the parent class's
        ``exists`` method. If the file exists, it opens the file using ROOT's TFile,
        retrieves the list of keys, and checks if the list contains any entries.

        Returns:
            bool: True if the file exists and contains at least one key, False otherwise.
        """
        if not super().exists():
            return False

        path = self.path

        import ROOT

        tfile = ROOT.TFile.Open(path)
        len_list_of_keys = len(tfile.GetListOfKeys())
        tfile.Close()
        return len_list_of_keys > 0

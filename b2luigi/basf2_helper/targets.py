import b2luigi


class ROOTLocalTarget(b2luigi.LocalTarget):
    def exists(self):
        if not super().exists():
            return False

        path = self.path

        import ROOT

        tfile = ROOT.TFile.Open(path)
        len_list_of_keys = len(tfile.GetListOfKeys())
        tfile.Close()
        return len_list_of_keys > 0
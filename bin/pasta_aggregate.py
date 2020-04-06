from logging import getLogger
import pickle

from pypasta import *

log = getLogger(__name__[-15:])


def aggregate(config, prog, argv):
    parser = argparse.ArgumentParser(prog=prog,
                                     description='aggregate commit and patch info')

    # aggregate patches for commits
    parser.add_argument('mode', default='patches',
                        choices=['patches', 'commits', 'responses'],
                        help='patches: '
                             'aggregate patches for commits \n'
                             'commits: '
                             'aggregate commits for clusters \n'
                             'responses: '
                             'aggregate responses to patch emails \n'
                             'default: %(default)s')

    parser.add_argument('-np', dest='num_patches', metavar='number of patches to aggregate by', type=int,
                        help='Number of patches to aggregate git commits by \n'
                             'E.g., -np 2 gives number of git commits with 2 patches')

    args = parser.parse_args(argv)
    mode = args.mode

    if config.mode != config.Mode.MBOX:
        log.error('Only works in Mbox mode')
        return -1

    repo = config.repo
    _, clustering = config.load_cluster()
    clustering.optimize()

    clust = []

    for d, u in clustering.iter_split():
        clust.append((d, u))

    if mode == 'patches':

        num_patches = args.num_patches
        if num_patches is not None:

            commits_with_num_patches = [u for (d, u) in clust
                                        if len(d) == num_patches]
            log.info('Number of commits with {} patch emails: {}'.format(num_patches, len(commits_with_num_patches)))

        else:

            commits_without_patches = [u for (d, u) in clust
                                       if len(d) == 0]

            commits_with_single_patch_version = [u for (d, u) in clust
                                                 if len(d) == 1]

            commits_with_two_patch_versions = [u for (d, u) in clust
                                               if len(d) == 2]

            log.info('Number of commits without any patch version: {}'.format(len(commits_without_patches)))
            log.info('Number of commits with a single patch version: {}'.format(len(commits_with_single_patch_version)))
            log.info('Number of commits two patch versions: {}'.format(len(commits_with_two_patch_versions)))

    if mode == 'commits':
        clusters_without_commits = [d for (d, u) in clust if len(u) == 0]

        log.info('Number of clusters of patch emails with no upstream commits: {}'
                 .format(len(clusters_without_commits)))

        clusters_with_one_commit = [d for (d, u) in clust if len(u) == 1 and len(d) > 0]

        log.info('Number of patch clusters (versions) with one upstream commit {}'
                 .format(len(clusters_with_one_commit)))

    if mode == 'responses':

        threads = repo.mbox.load_threads()

        def _load_responses_dict(response_list, pending_responses):
            if not pending_responses:
                return
            next_msg = pending_responses.pop(0)
            try:
                next_responses = list(threads.reply_to_map[next_msg])
                pending_responses.extend(next_responses)
                for resp in next_responses:
                    resp_dict = {'parent': next_msg, 'resp_msg_id': resp, 'message': repo.mbox.get_raws(resp)}
                    response_list.append(resp_dict)
                _load_responses_dict(response_list, pending_responses)
            except KeyError:
                log.info("The email {} has no response_list".format(next_msg))
                return

        clusters = []
        for d, u in clustering.iter_split():
            # Handle upstream commits without patches
            if not d:
                cluster_dict = {}
                try:
                    cluster_dict['cluster_id'] = clustering.lookup[next(iter(u))]
                    cluster_dict['upstream'] = u
                    cluster_dict['patch_id'] = None
                    cluster_dict['responses'] = None
                    clusters.append(cluster_dict)
                except KeyError:
                    log.warning("No downstream or upstream found, bad entry?...Skipping")
            for patch_id in d:
                cluster_dict = {}
                # Handle cluster id for patch
                try:
                    cluster_dict['cluster_id'] = clustering.lookup[patch_id]
                except KeyError:
                    log.info("No downstream patches, try upstream to get cluster id")
                    try:
                        cluster_dict['cluster_id'] = clustering.lookup[next(iter(u))]
                    except KeyError:
                        log.warning("No upstream, problem finding cluster id in clustering lookup for commit {}"
                                    .format(next(iter(u))))
                        cluster_dict['cluster_id'] = None

                # Handle downstream: list of upstream commits
                cluster_dict['patch_id'] = patch_id
                # Handle upstream: list of upstream commits
                cluster_dict['upstream'] = u

                # Handle responses
                response_lst = []
                pending_lst = [patch_id]
                _load_responses_dict(response_lst, pending_lst)
                cluster_dict['responses'] = response_lst
                clusters.append(cluster_dict)

        with open('patch_responses.pickle', 'wb') as handle:
            pickle.dump(clusters, handle, protocol=pickle.HIGHEST_PROTOCOL)
        log.info("Done writing response info for {} patch/commit entries!".format(len(clusters)))
        log.info("Total clusters found by pasta: {}".format(len(clust)))
